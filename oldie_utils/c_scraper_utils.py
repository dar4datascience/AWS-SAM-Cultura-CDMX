import asyncio
import re
import json
from playwright.async_api import expect


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

                    // Description
                    const desc = wrapper.querySelector('.cdmx-billboard-page-event-description-container');
                    data["description"] = desc ? Array.from(desc.querySelectorAll("p"))
                        .map(p => p.innerText.trim()).filter(Boolean) : None;

                    // Info
                    const info = wrapper.querySelector('.cdmx-billboard-page-event-info-container');
                    if (info) {
                        const bordered = info.querySelector('.cdmx-billboard-page-event-info-container-bordered');
                        data["info"] = bordered ? Array.from(bordered.querySelectorAll("ul li"))
                            .map(li => li.innerText.trim()).filter(Boolean) : None
                    } else {
                        data["info"] = None
                    }

                    // Schedule
                    const sched = wrapper.querySelector('.cdmx-billboard-page-event-schedule-container');
                    if (sched) {
                        const dateEl = sched.querySelector('#cdmx-billboard-current-date-label');
                        const hourEl = sched.querySelector('#cdmx-billboard-current-hour-label');
                        data["schedule"] = {
                            "date": dateEl ? dateEl.innerText.trim() : None,
                            "hour": hourEl ? hourEl.innerText.trim() : None
                        }
                    } else {
                        data["schedule"] = None
                    }

                    // Location
                    const loc = wrapper.querySelector('.cdmx-billboard-page-event-location-container');
                    if (loc) {
                        const span = loc.querySelector("span");
                        data["location"] = span ? span.innerText.trim() : None
                    } else {
                        data["location"] = None
                    }

                    return data;
                }
            """)
            return html_content or {"description": None, "info": None, "schedule": None, "location": None}

        except Exception as e:
            if "Execution context was destroyed" in str(e):
                print(f"Retrying inner page scrape, attempt {attempt+1}")
                await asyncio.sleep(1)
                continue
            print(f"Error scraping inner page: {e}")
            return {"description": None, "info": None, "schedule": None, "location": None}

    return {"description": None, "info": None, "schedule": None, "location": None}



async def scrape_card_on_page(browser, page_number, card_index, sem, max_retries=3):
    """
    Scrape a single card from a given page number safely with retries.
    Opens a fresh browser context, clicks the card, and extracts detail page info.
    """
    async with sem:
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(60_000)  # 60 seconds

        url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"

        for attempt in range(1, max_retries + 1):
            try:
                # Navigate with 60s timeout
                await page.goto(url, wait_until="load", timeout=60_000)
                await scroll_to_bottom(page)

                # Click the card safely
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

                # Scrape inner page with retries
                detail_data = await scrape_inner_page(page)
                detail_url = page.url

                await context.close()
                return {
                    "page_number": page_number,
                    "card_index": card_index,
                    "detail_url": detail_url,
                    **detail_data,
                }

            except Exception as e:
                print(f"Attempt {attempt} failed for card {card_index} on page {page_number}: {e}")
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
                await asyncio.sleep(2)  # wait a bit before retry



async def save_page_data(data, page_number):
    """Save scraped data of one page to JSON file."""
    filename = f"cultura_pages/events_page_{page_number}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved page {page_number} data to {filename}")
