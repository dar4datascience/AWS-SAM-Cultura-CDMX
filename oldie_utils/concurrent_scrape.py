import asyncio
from playwright.async_api import async_playwright, expect

MAX_CONCURRENT_CONTEXTS = 3  # Limit number of simultaneous contexts

async def scrape_inner_page(page):
    """
    Scrape inner page sections of an event, including nested elements,
    taking into account the generic page container wrapper.
    """
    sections = [
        "cdmx-billboard-page-event-description-container",
        "cdmx-billboard-page-event-info-container",
        "cdmx-billboard-page-event-schedule-container",
        "cdmx-billboard-page-event-location-container"
    ]

    data = {}
    try:
        html_content = await page.evaluate(f"""
            () => {{
                const wrapper = document.querySelector('.cdmx-billboard-generic-page-container');
                if (!wrapper) return null;
                const data = {{}};
                const sectionClasses = {sections};
                sectionClasses.forEach(cls => {{
                    const el = wrapper.querySelector('.' + cls);
                    data[cls] = el ? el.outerHTML : null;
                }});
                return data;
            }}
        """)
        if html_content:
            data = html_content
    except Exception as e:
        print(f"Failed to scrape inner page: {e}")
        for cls in sections:
            data[cls] = None

    return data


async def scrape_card_in_context(browser, card_index, sem):
    """Scrape a single card in its own context using direct JS evaluation."""
    async with sem:
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto("https://cartelera.cdmx.gob.mx/busqueda")
        await expect(page.locator(".cdmx-billboard-home-top-container-title-container-titles")).to_be_visible()
        await page.get_by_text("eventos para ti").click()
        await page.wait_for_timeout(1000)

        # Scroll to load cards
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(500)

        # Click the specific card by index
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

        # Scrape inner page sections
        detail_data = await scrape_inner_page(page)
        detail_url = page.url

        # Optional: go back to main page
        # await page.evaluate("""
        #     () => {
        #         const btn = document.querySelector('#cdmx-billboard-return-home-button');
        #         if(btn) btn.click();
        #     }
        # """)
        await page.wait_for_timeout(500)

        await context.close()
        return {"card_index": card_index, "detail_url": detail_url, "detail_data": detail_data}

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        # Open main page to count cards
        page = await browser.new_page()
        await page.goto("https://cartelera.cdmx.gob.mx/busqueda")
        await expect(page.locator(".cdmx-billboard-home-top-container-title-container-titles")).to_be_visible()
        await page.get_by_text("eventos para ti").click()
        await page.wait_for_timeout(1000)

        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(500)

        cards = page.locator("#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container")
        num_cards = await cards.count()
        print(f"Found {num_cards} cards on this page")

        await page.close()

        # Semaphore to limit concurrency
        sem = asyncio.Semaphore(MAX_CONCURRENT_CONTEXTS)
        tasks = [scrape_card_in_context(browser, i, sem) for i in range(num_cards)]
        results = await asyncio.gather(*tasks)

        for r in results:
            print(f"Card {r['card_index']} URL: {r['detail_url']}")
            for k, v in r['detail_data'].items():
                print(f"  Section {k}: {len(v) if v else 'None'} characters")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
