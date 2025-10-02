import asyncio
import time
from playwright.async_api import async_playwright
from c_scraper_utils import scroll_to_bottom, scrape_card_on_page, save_page_data

MAX_CONCURRENT_PAGES = 5   # how many pages to scrape at once
MAX_CONCURRENT_CARDS = 9  # how many cards to scrape per page at once


async def detect_last_page(browser):
    """Open the site once and detect the last page number from paginator."""
    page = await browser.new_page()
    await page.goto("https://cartelera.cdmx.gob.mx/busqueda")
    await scroll_to_bottom(page)

    paginator_last = page.locator("#cdmx-billboard-event-paginator li.page.btn[jp-role='last']")
    last_page_attr = await paginator_last.get_attribute("jp-data")
    last_page = int(last_page_attr) if last_page_attr else 1

    await page.close()
    return last_page


async def scrape_page_cards(browser, page_number, page_sem, card_sem, max_retries=3):
    """Scrape all cards from one page, concurrently, with retries and increased timeout."""
    async with page_sem:
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(120_000)  # 120 seconds

        url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
        for attempt in range(1, max_retries + 1):
            try:
                await page.goto(url, wait_until="load")
                await scroll_to_bottom(page)
                break  # success, exit retry loop
            except Exception as e:
                print(f"Attempt {attempt} failed navigating page {page_number}: {e}")
                if attempt == max_retries:
                    print(f"Failed to load page {page_number} after {max_retries} attempts.")
                    await context.close()
                    return []
                await asyncio.sleep(2)  # wait a bit before retry

        # count cards
        card_count = await page.locator(
            "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
        ).count()
        print(f"Page {page_number} has {card_count} cards")

        # spawn tasks for each card
        tasks = [scrape_card_on_page(browser, page_number, i, card_sem) for i in range(card_count)]
        page_results = await asyncio.gather(*tasks)

        await save_page_data(page_results, page_number)
        await context.close()
        return page_results



async def scrape_all_cards():
    """Main entrypoint: scrape all pages concurrently."""
    start_time = time.perf_counter()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        last_page = await detect_last_page(browser)
        print(f"Detected last page: {last_page}")

        page_sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        card_sem = asyncio.Semaphore(MAX_CONCURRENT_CARDS)

        # Launch page tasks in parallel
        tasks = [
            scrape_page_cards(browser, page_number, page_sem, card_sem)
            for page_number in range(1, last_page + 1)
        ]
        await asyncio.gather(*tasks)

        await browser.close()

    end_time = time.perf_counter()
    print(f"\nTotal scraping time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(scrape_all_cards())
