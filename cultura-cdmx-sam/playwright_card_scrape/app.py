import asyncio
import json
import os
import boto3
from datetime import datetime
from playwright.async_api import async_playwright

# Initialize S3 client outside the handler for re-use
s3 = boto3.client("s3")

async def scroll_to_bottom(page):
    await page.evaluate("""
        () => {
            return new Promise(resolve => {
                let totalHeight = 0;
                const distance = 500;
                const timer = setInterval(() => {
                    const scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    if (totalHeight >= scrollHeight) {
                        clearInterval(timer);
                        resolve();
                    }
                }, 200);
            });
        }
    """)
    await page.wait_for_timeout(1000)

async def scrape_inner_page(page, retries=2):
    for attempt in range(retries):
        try:
            html_content = await page.evaluate("""
                () => {
                    const wrapper = document.querySelector('.cdmx-billboard-generic-page-container');
                    if (!wrapper) return null;

                    const data = {};
                    const desc = wrapper.querySelector('.cdmx-billboard-page-event-description-container');
                    data["description"] = desc ? Array.from(desc.querySelectorAll("p"))
                        .map(p => p.innerText.trim()).filter(Boolean) : null;

                    const info = wrapper.querySelector('.cdmx-billboard-page-event-info-container');
                    if (info) {
                        const bordered = info.querySelector('.cdmx-billboard-page-event-info-container-bordered');
                        data["info"] = bordered ? Array.from(bordered.querySelectorAll("ul li"))
                            .map(li => li.innerText.trim()).filter(Boolean) : null;
                    } else {
                        data["info"] = null;
                    }

                    const sched = wrapper.querySelector('.cdmx-billboard-page-event-schedule-container');
                    if (sched) {
                        const dateEl = sched.querySelector('#cdmx-billboard-current-date-label');
                        const hourEl = sched.querySelector('#cdmx-billboard-current-hour-label');
                        data["schedule"] = {
                            "date": dateEl ? dateEl.innerText.trim() : null,
                            "hour": hourEl ? hourEl.innerText.trim() : null
                        };
                    } else {
                        data["schedule"] = None;
                    }

                    const loc = wrapper.querySelector('.cdmx-billboard-page-event-location-container');
                    if (loc) {
                        const span = loc.querySelector("span");
                        data["location"] = span ? span.innerText.trim() : null;
                    } else {
                        data["location"] = None;
                    }

                    return data;
                }
            """)
            return html_content or {"description": None, "info": None, "schedule": None, "location": None}
        except Exception:
            await asyncio.sleep(1)
    return {"description": None, "info": None, "schedule": None, "location": None}

async def scrape_card_on_page(browser, page_number, card_index, sem):
    async with sem:
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(60_000)
        url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"

        await page.goto(url, wait_until="load")
        await scroll_to_bottom(page)

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
        detail_url = page.url
        await context.close()
        return {"page_number": page_number, "card_index": card_index, "detail_url": detail_url, **detail_data}

async def scrape_page_cards(browser, page_number, card_sem):
    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_navigation_timeout(120_000)

    url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
    await page.goto(url, wait_until="load")
    await scroll_to_bottom(page)

    card_count = await page.locator(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    ).count()
    tasks = [scrape_card_on_page(browser, page_number, i, card_sem) for i in range(card_count)]
    results = await asyncio.gather(*tasks)
    await context.close()
    return results

async def run_scraper(page_number: int):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process"
            ],
        )
        card_sem = asyncio.Semaphore(9)
        results = await scrape_page_cards(browser, page_number, card_sem)
        await browser.close()
        return results

def handler(event, context):
    """AWS Lambda handler to scrape a page and store results in S3."""
    page_number = int(event.get("page_number", 1))
    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        return {"statusCode": 500, "body": "Environment variable BUCKET_NAME not set."}

    results = asyncio.get_event_loop().run_until_complete(run_scraper(page_number))

    # Store in S3
    snapshot_date = datetime.utcnow().strftime("%Y%m%d")

    s3_key = f"snapshot_date/{snapshot_date}/events_page_{page_number}_{datetime.utcnow().isoformat()}.json"
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(results, ensure_ascii=False))

    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Scraped page {page_number} and saved to s3://{bucket_name}/{s3_key}"})
    }
