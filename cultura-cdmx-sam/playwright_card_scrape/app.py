import asyncio
import json
import os
import boto3
from datetime import datetime
from playwright.async_api import async_playwright

# Initialize S3 client outside the handler for re-use
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

async def scrape_inner_page(page, retries=3):
    """Scrape event details from inner page with retries."""
    for attempt in range(retries):
        try:
            data = await page.evaluate("""
                () => {
                    const wrapper = document.querySelector('.cdmx-billboard-generic-page-container');
                    if (!wrapper) return null;

                    const d = {};
                    const desc = wrapper.querySelector('.cdmx-billboard-page-event-description-container');
                    d.description = desc ? Array.from(desc.querySelectorAll("p"))
                        .map(p => p.innerText.trim()).filter(Boolean) : null;

                    const info = wrapper.querySelector('.cdmx-billboard-page-event-info-container');
                    if (info) {
                        const bordered = info.querySelector('.cdmx-billboard-page-event-info-container-bordered');
                        d.info = bordered ? Array.from(bordered.querySelectorAll("ul li"))
                            .map(li => li.innerText.trim()).filter(Boolean) : null;
                    } else d.info = null;

                    const sched = wrapper.querySelector('.cdmx-billboard-page-event-schedule-container');
                    if (sched) {
                        const dateEl = sched.querySelector('#cdmx-billboard-current-date-label');
                        const hourEl = sched.querySelector('#cdmx-billboard-current-hour-label');
                        d.schedule = {
                            date: dateEl ? dateEl.innerText.trim() : None,
                            hour: hourEl ? hourEl.innerText.trim() : None
                        };
                    } else d.schedule = null;

                    const loc = wrapper.querySelector('.cdmx-billboard-page-event-location-container');
                    if (loc) {
                        const span = loc.querySelector("span");
                        d.location = span ? span.innerText.trim() : null;
                    } else d.location = null;

                    return d;
                }
            """)
            return data or {"description": None, "info": None, "schedule": None, "location": None}
        except Exception as e:
            # Retry on transient failures
            if "Execution context was destroyed" in str(e) or "Target page" in str(e):
                await asyncio.sleep(1)
                continue
            print(f"Error scraping inner page: {e}")
            return {"description": None, "info": None, "schedule": None, "location": None}
    return {"description": None, "info": None, "schedule": None, "location": None}

async def scrape_card(context, page_number, card_index, sem, max_retries=3):
    """Scrape a single card safely using a shared browser context with concurrency control."""
    async with sem:
        for attempt in range(1, max_retries + 1):
            page = await context.new_page()
            page.set_default_navigation_timeout(60_000)
            try:
                url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
                await page.goto(url, wait_until="load", timeout=60_000)
                await scroll_to_bottom(page)

                # Click card
                await page.evaluate(f"""
                    () => {{
                        const cards = document.querySelectorAll(
                            '#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container'
                        );
                        if (cards.length > {card_index}) {{
                            cards[{card_index}].scrollIntoView();
                            cards[{card_index}].click();
                        }}
                    }}
                """)
                await page.wait_for_selector(".cdmx-billboard-generic-page-container", timeout=15_000)

                # Scrape inner page
                detail_data = await scrape_inner_page(page)
                detail_data.update({
                    "page_number": page_number,
                    "card_index": card_index,
                    "detail_url": page.url
                })

                await page.close()
                return detail_data

            except Exception as e:
                await page.close()
                print(f"Attempt {attempt} failed for card {card_index} on page {page_number}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2)
                else:
                    return {
                        "page_number": page_number,
                        "card_index": card_index,
                        "detail_url": None,
                        "description": None,
                        "info": None,
                        "schedule": None,
                        "location": None,
                    }

async def scrape_page_cards(browser, page_number, max_concurrent=4):
    """Scrape all cards concurrently with one browser and multiple contexts."""
    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_navigation_timeout(120_000)

    url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
    await page.goto(url, wait_until="load")
    await scroll_to_bottom(page)

    card_count = await page.locator(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    ).count()
    await page.close()

    sem = asyncio.Semaphore(max_concurrent)
    tasks = [scrape_card(context, page_number, i, sem) for i in range(card_count)]
    results = await asyncio.gather(*tasks)
    await context.close()
    return results

async def run_scraper(page_number: int, max_concurrent=4):
    """Launch browser and scrape a page concurrently."""
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
        results = await scrape_page_cards(browser, page_number, max_concurrent=max_concurrent)
        await browser.close()
        return results

def handler(event, context):
    """AWS Lambda handler to scrape a page concurrently and store pretty JSON in S3."""
    page_number = int(event.get("page_number", 1))
    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        return {"statusCode": 500, "body": "Environment variable BUCKET_NAME not set."}

    results = asyncio.get_event_loop().run_until_complete(run_scraper(page_number, max_concurrent=4))

    snapshot_date = datetime.utcnow().strftime("%Y%m%d")
    s3_key = f"snapshot_date/{snapshot_date}/events_page_{page_number}_{datetime.utcnow().isoformat()}.json"

    # Pretty-print JSON in S3
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
