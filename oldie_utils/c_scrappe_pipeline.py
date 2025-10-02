import asyncio
import json
import time
from playwright.async_api import async_playwright
from scraper_utils import scrape_inner_page, navigate_to_site, scroll_to_bottom

MAX_CONCURRENT_PAGES = 10  # Limit simultaneous browser contexts

async def scrape_card_on_page(browser, page_number, card_index, sem):
    """Scrape a card on a given page number in its own browser context."""
    async with sem:
        context = await browser.new_context()
        page = await context.new_page()

        # Go to the specific page
        await page.goto("https://cartelera.cdmx.gob.mx/busqueda")
        await navigate_to_site(page)

        # Navigate to the desired page number
        if page_number > 1:
            await page.evaluate(f"""
                () => {{
                    const btn = Array.from(document.querySelectorAll('#cdmx-billboard-event-paginator li.page.btn'))
                        .find(el => el.innerText == '{page_number}');
                    if (btn) btn.click();
                }}
            """)
            await page.wait_for_timeout(1500)
            await scroll_to_bottom(page)

        # Click specific card
        await page.evaluate(f"""
            () => {{
                const cards = document.querySelectorAll('#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container');
                if (cards.length > {card_index}) {{
                    cards[{card_index}].scrollIntoView();
                    cards[{card_index}].click();
                }}
            }}
        """)
        await page.wait_for_timeout(1500)

        detail_data = await scrape_inner_page(page)
        detail_url = page.url

        await context.close()
        return {"page_number": page_number, "card_index": card_index, "detail_url": detail_url, **detail_data}

async def save_page_data(data, page_number):
    filename = f"events_page_{page_number}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved page {page_number} data to {filename}")

async def scrape_all_cards():
    start_time = time.perf_counter()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        temp_page = await browser.new_page()
        await temp_page.goto("https://cartelera.cdmx.gob.mx/busqueda")
        await navigate_to_site(temp_page)

        # Detect last page
        paginator_last = temp_page.locator("#cdmx-billboard-event-paginator li.page.btn[jp-role='last']")
        last_page_attr = await paginator_last.get_attribute("jp-data")
        last_page = int(last_page_attr) if last_page_attr else 1
        print(f"Detected last page: {last_page}")

        sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

        for page_number in range(1, last_page + 1):
            print(f"\nProcessing page {page_number}...")
            # Navigate to page_number to count cards
            await temp_page.goto("https://cartelera.cdmx.gob.mx/busqueda")
            await navigate_to_site(temp_page)
            if page_number > 1:
                await temp_page.evaluate(f"""
                    () => {{
                        const btn = Array.from(document.querySelectorAll('#cdmx-billboard-event-paginator li.page.btn'))
                            .find(el => el.innerText == '{page_number}');
                        if (btn) btn.click();
                    }}
                """)
                await temp_page.wait_for_timeout(1500)
                await scroll_to_bottom(temp_page)

            card_count = await temp_page.locator(
                "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
            ).count()
            print(f"Page {page_number} has {card_count} cards")

            # Launch scraping tasks for all cards on this page
            tasks = [scrape_card_on_page(browser, page_number, i, sem) for i in range(card_count)]
            page_results = await asyncio.gather(*tasks)

            # Save each page immediately
            await save_page_data(page_results, page_number)

        await temp_page.close()
        await browser.close()

    end_time = time.perf_counter()
    print(f"\nTotal scraping time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(scrape_all_cards())
