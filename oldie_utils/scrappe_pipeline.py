# scrape_pipeline.py
import asyncio
import time
from playwright.async_api import async_playwright
from scraper_utils import scrape_page, navigate_to_site, go_to_next_page, save_data

async def scrape_all_pages():
    start_time = time.perf_counter()  # Start timing
    all_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        await navigate_to_site(page)

        # Detect last page
        paginator_last = page.locator("#cdmx-billboard-event-paginator li.page.btn[jp-role='last']")
        last_page_attr = await paginator_last.get_attribute("jp-data")
        last_page = int(last_page_attr) if last_page_attr else 1
        print(f"Detected last page: {last_page}")

        current_page = 1
        while current_page <= last_page:
            print(f"Scraping page {current_page}")
            page_data = await scrape_page(page)
            all_data.extend(page_data)
            if not await go_to_next_page(page, current_page):
                break
            current_page += 1

        await save_data(all_data)
        await context.close()
        await browser.close()

    end_time = time.perf_counter()  # End timing
    total_time = end_time - start_time
    print(f"\nTotal scraping time: {total_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(scrape_all_pages())
