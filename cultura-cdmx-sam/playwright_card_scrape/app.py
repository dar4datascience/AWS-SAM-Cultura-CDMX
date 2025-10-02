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
                            date: dateEl ? dateEl.innerText.trim() : null,
                            hour: hourEl ? hourEl.innerText.trim() : null
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
            if "Execution context was destroyed" in str(e) or "Target page" in str(e):
                await asyncio.sleep(1)
                continue
            print(f"Error scraping inner page: {e}")
    return {"description": None, "info": None, "schedule": None, "location": None}


async def scrape_card(page, page_number, card_index, max_retries=3):
    """Scrape a single card using an existing page."""
    for attempt in range(1, max_retries + 1):
        try:
            # Scroll to card and click
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

            detail_data = await scrape_inner_page(page)
            detail_data.update({
                "page_number": page_number,
                "card_index": card_index,
                "detail_url": page.url
            })
            return detail_data
        except Exception as e:
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


async def scrape_page_cards(browser, page_number, max_concurrent=3):
    """Scrape all cards concurrently using a single context per page number."""
    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_navigation_timeout(120_000)

    url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
    await page.goto(url, wait_until="load")
    await scroll_to_bottom(page)

    card_count = await page.locator(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    ).count()

    sem = asyncio.Semaphore(max_concurrent)

    async def scrape_card_semaphore(i):
        async with sem:
            new_page = await context.new_page()
            try:
                return await scrape_card(new_page, page_number, i)
            finally:
                await new_page.close()

    tasks = [scrape_card_semaphore(i) for i in range(card_count)]
    results = await asyncio.gather(*tasks)
    await context.close()
    return results


async def run_scraper(page_number: int, max_concurrent=3):
    """Launch browser and scrape a page concurrently."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process"
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

    results = asyncio.run(run_scraper(page_number, max_concurrent=3))

    snapshot_date = datetime.utcnow().strftime("%Y%m%d")
    s3_key = f"snapshot_date/{snapshot_date}/events_page_{page_number}_{datetime.utcnow().isoformat()}.json"

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
