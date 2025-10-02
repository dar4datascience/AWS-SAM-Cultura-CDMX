import asyncio
import json
from playwright.async_api import async_playwright


async def scroll_to_bottom(page):
    """Scroll to the bottom of the page incrementally to trigger lazy-loaded content."""
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
    """Scrape event details from inner page with retries."""
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
                        data["schedule"] = null;
                    }

                    const loc = wrapper.querySelector('.cdmx-billboard-page-event-location-container');
                    if (loc) {
                        const span = loc.querySelector("span");
                        data["location"] = span ? span.innerText.trim() : null;
                    } else {
                        data["location"] = null;
                    }

                    return data;
                }
            """)
            return html_content or {"description": None, "info": None, "schedule": None, "location": None}

        except Exception as e:
            if "Execution context was destroyed" in str(e):
                await asyncio.sleep(1)
                continue
            return {"description": None, "info": None, "schedule": None, "location": None}

    return {"description": None, "info": None, "schedule": None, "location": None}


async def scrape_card_on_page(browser, page_number, card_index, sem, max_retries=3):
    """Scrape a single card from a given page number safely with retries."""
    async with sem:
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(60_000)

        url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"

        for attempt in range(1, max_retries + 1):
            try:
                await page.goto(url, wait_until="load", timeout=60_000)
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
                return {
                    "page_number": page_number,
                    "card_index": card_index,
                    "detail_url": detail_url,
                    **detail_data,
                }

            except Exception:
                if attempt == max_retries:
                    await context.close()
                    return {
                        "page_number": page_number,
                        "card_index": card_index,
                        "detail_url": None,
                        "description": None,
                        "info": None,
                        "schedule": None,
                        "location": None,
                    }
                await asyncio.sleep(2)


async def scrape_page_cards(browser, page_number, card_sem):
    """Scrape all cards from one page concurrently."""
    context = await browser.new_context()
    page = await context.new_page()
    page.set_default_navigation_timeout(120_000)

    url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
    await page.goto(url, wait_until="load")
    await scroll_to_bottom(page)

    # count cards
    card_count = await page.locator(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    ).count()
    print(f"Page {page_number} has {card_count} cards")

    # spawn tasks for each card
    tasks = [scrape_card_on_page(browser, page_number, i, card_sem) for i in range(card_count)]
    page_results = await asyncio.gather(*tasks)

    await context.close()
    return page_results


async def run_scraper(page_number: int):
    """Run the scraper for a single page."""
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
    """AWS Lambda entrypoint."""
    page_number = int(event.get("page_number", 1))
    results = asyncio.get_event_loop().run_until_complete(run_scraper(page_number))

    return {
        "statusCode": 200,
        "body": json.dumps(results, ensure_ascii=False)
    }
