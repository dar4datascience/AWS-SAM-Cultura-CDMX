import asyncio
import json
import os
import boto3
from datetime import datetime
from playwright.async_api import async_playwright

s3 = boto3.client("s3")


async def scroll_to_bottom(page, distance=500, timeout_ms=1000):
    """Scroll to the bottom incrementally to trigger lazy-loaded content."""
    await page.evaluate(f"""
        () => {{
            return new Promise(resolve => {{
                let totalHeight = 0;
                const distance = {distance};
                const timer = setInterval(() => {{
                    const scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    if (totalHeight >= scrollHeight) {{
                        clearInterval(timer);
                        resolve();
                    }}
                }}, 200);
            }});
        }}
    """)
    await page.wait_for_timeout(timeout_ms)


async def scrape_inner_page(page, retries=2):
    """Scrape event details from inner page with retries."""
    for attempt in range(retries):
        try:
            data = await page.evaluate("""
                () => {
                    const wrapper = document.querySelector('.cdmx-billboard-generic-page-container');
                    if (!wrapper) return null;
                    const d = {};
                    const desc = wrapper.querySelector('.cdmx-billboard-page-event-description-container');
                    d.description = desc ? Array.from(desc.querySelectorAll("p"), p => p.innerText.trim()).filter(Boolean) : null;

                    const info = wrapper.querySelector('.cdmx-billboard-page-event-info-container');
                    if (info) {
                        const bordered = info.querySelector('.cdmx-billboard-page-event-info-container-bordered');
                        d.info = bordered ? Array.from(bordered.querySelectorAll("ul li"), li => li.innerText.trim()).filter(Boolean) : null;
                    } else d.info = None;

                    const sched = wrapper.querySelector('.cdmx-billboard-page-event-schedule-container');
                    d.schedule = sched ? {
                        date: sched.querySelector('#cdmx-billboard-current-date-label')?.innerText.trim() || None,
                        hour: sched.querySelector('#cdmx-billboard-current-hour-label')?.innerText.trim() || None
                    } : None;

                    const loc = wrapper.querySelector('.cdmx-billboard-page-event-location-container');
                    d.location = loc ? loc.querySelector("span")?.innerText.trim() : None;

                    return d;
                }
            """)
            return data or {"description": None, "info": None, "schedule": None, "location": None}
        except Exception as e:
            if "Execution context was destroyed" in str(e):
                await asyncio.sleep(1)
                continue
            print(f"Error scraping inner page: {e}")
            return {"description": None, "info": None, "schedule": None, "location": None}
    return {"description": None, "info": None, "schedule": None, "location": None}


async def scrape_page_sequential(browser, page_number: int):
    """
    Scrape all cards sequentially on a page using a single browser context.
    Memory-safe for AWS Lambda.
    """
    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_navigation_timeout(60_000)

    url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
    await page.goto(url, wait_until="load")
    await scroll_to_bottom(page)

    card_count = await page.locator(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    ).count()

    results = []

    for i in range(card_count):
        try:
            # Scroll to card and click (same page)
            await page.evaluate(f"""
                () => {{
                    const cards = document.querySelectorAll(
                        '#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container'
                    );
                    if (cards.length > {i}) {{
                        cards[{i}].scrollIntoView();
                        cards[{i}].click();
                    }}
                }}
            """)

            # Wait for the detail container to appear
            await page.wait_for_selector(".cdmx-billboard-generic-page-container", timeout=30_000)

            # Extract inner page data
            data = await scrape_inner_page(page)

            results.append({
                "page_number": page_number,
                "card_index": i,
                "detail_url": page.url,
                **data
            })

        except Exception as e:
            print(f"Failed to scrape card {i} on page {page_number}: {e}")
            results.append({
                "page_number": page_number,
                "card_index": i,
                "detail_url": None,
                "description": None,
                "info": None,
                "schedule": None,
                "location": None
            })

    await context.close()
    return results


async def run_scraper(page_number: int):
    """Launch Playwright browser and scrape the page sequentially."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu"
            ]
        )
        results = await scrape_page_sequential(browser, page_number)
        await browser.close()
        return results


def handler(event, context):
    """AWS Lambda handler."""
    page_number = int(event.get("page_number", 1))
    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        return {"statusCode": 500, "body": "Environment variable BUCKET_NAME not set."}

    results = asyncio.run(run_scraper(page_number))

    snapshot_date = datetime.utcnow().strftime("%Y%m%d")
    s3_key = f"snapshot_date/{snapshot_date}/events_page_{page_number}_{datetime.utcnow().isoformat().replace(':','-')}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(results, ensure_ascii=False, indent=2)
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Scraped page {page_number} and saved to s3://{bucket_name}/{s3_key}"
        })
    }
