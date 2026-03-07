"""
Local diagnostic test for cartelera.cdmx.gob.mx scraping.

Runs Playwright directly (no Lambda/AWS) and prints exactly what each
CSS selector finds, so broken/changed selectors can be identified quickly.

Usage:
    pip install playwright==1.54.0
    playwright install chromium
    python scripts/test_scraper.py [page_number] [max_cards]

Defaults: page_number=1, max_cards=3
"""

import asyncio
import json
import sys
import time
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

PAGE_NUMBER = int(sys.argv[1]) if len(sys.argv) > 1 else 1
MAX_CARDS = int(sys.argv[2]) if len(sys.argv) > 2 else 3

CARD_SELECTOR = "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
DETAIL_CONTAINER = ".cdmx-billboard-generic-page-container"
BASE_URL = "https://cartelera.cdmx.gob.mx/busqueda"


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def ok(msg):   print(f"  [OK]   {msg}")
def fail(msg): print(f"  [FAIL] {msg}")
def info(msg): print(f"  [INFO] {msg}")


async def check_page_numbers(page):
    section("TEST 1 — CulturaPageCheck: paginator selector")
    url = BASE_URL
    info(f"Navigating to {url}")
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)

    # Scroll to trigger lazy-load
    await page.evaluate("""
        () => new Promise(resolve => {
            let total = 0;
            const t = setInterval(() => {
                window.scrollBy(0, 500);
                total += 500;
                if (total >= document.body.scrollHeight) { clearInterval(t); resolve(); }
            }, 200);
        })
    """)
    await page.wait_for_timeout(1000)

    # Check paginator container
    container = await page.query_selector("#cdmx-billboard-event-paginator")
    if container:
        ok("Paginator container found: #cdmx-billboard-event-paginator")
    else:
        fail("Paginator container NOT found: #cdmx-billboard-event-paginator")

    # Check for any page buttons
    buttons = await page.query_selector_all("#cdmx-billboard-event-paginator li.page.btn")
    info(f"Page buttons found (li.page.btn): {len(buttons)}")

    # Check last page button
    last_btn = await page.query_selector("#cdmx-billboard-event-paginator li.page.btn[jp-role='last']")
    if last_btn:
        jp_data = await last_btn.get_attribute("jp-data")
        ok(f"Last page button found — jp-data='{jp_data}'")
    else:
        fail("Last page button NOT found: li.page.btn[jp-role='last']")
        if buttons:
            info("Dumping all page button attributes:")
            for i, btn in enumerate(buttons[:5]):
                role = await btn.get_attribute("jp-role")
                data = await btn.get_attribute("jp-data")
                text = (await btn.inner_text()).strip()
                info(f"  btn[{i}] jp-role='{role}' jp-data='{data}' text='{text}'")


async def check_card_listing(page):
    section("TEST 2 — Card listing page: card selector")
    url = f"{BASE_URL}?tipo=ALL&pagina={PAGE_NUMBER}"
    info(f"Navigating to {url}")
    await page.goto(url, wait_until="domcontentloaded")

    # Scroll
    await page.evaluate("""
        () => new Promise(resolve => {
            let total = 0;
            const t = setInterval(() => {
                window.scrollBy(0, 500);
                total += 500;
                if (total >= document.body.scrollHeight) { clearInterval(t); resolve(); }
            }, 200);
        })
    """)
    await page.wait_for_timeout(1000)

    try:
        await page.wait_for_selector(CARD_SELECTOR, timeout=20_000)
        ok(f"Card selector found: {CARD_SELECTOR}")
    except PlaywrightTimeoutError:
        fail(f"Card selector NOT found within 20s: {CARD_SELECTOR}")
        info("Dumping page title and first 500 chars of body text:")
        title = await page.title()
        body_text = await page.evaluate("() => document.body.innerText.slice(0, 500)")
        info(f"  title: {title}")
        info(f"  body: {body_text}")
        return 0

    count = await page.locator(CARD_SELECTOR).count()
    ok(f"Cards found on page {PAGE_NUMBER}: {count}")
    return count


async def check_card_detail(page, card_index: int):
    section(f"TEST 3.{card_index} — Card {card_index}: click + detail selectors")
    url = f"{BASE_URL}?tipo=ALL&pagina={PAGE_NUMBER}"

    # Re-navigate to listing (each card test starts fresh)
    await page.goto(url, wait_until="domcontentloaded")
    await page.evaluate("""
        () => new Promise(resolve => {
            let total = 0;
            const t = setInterval(() => {
                window.scrollBy(0, 500);
                total += 500;
                if (total >= document.body.scrollHeight) { clearInterval(t); resolve(); }
            }, 200);
        })
    """)
    await page.wait_for_timeout(1000)
    await page.wait_for_selector(CARD_SELECTOR, timeout=20_000)

    card = page.locator(CARD_SELECTOR).nth(card_index)
    await card.scroll_into_view_if_needed()

    t0 = time.time()
    try:
        await card.click(timeout=10_000)
        ok(f"Card {card_index} clicked in {time.time()-t0:.1f}s — URL: {page.url}")
    except PlaywrightTimeoutError:
        fail(f"Card {card_index} click timed out after {time.time()-t0:.1f}s")
        return

    # Wait for detail container
    t0 = time.time()
    try:
        await page.wait_for_selector(DETAIL_CONTAINER, timeout=20_000)
        ok(f"Detail container appeared in {time.time()-t0:.1f}s: {DETAIL_CONTAINER}")
    except PlaywrightTimeoutError:
        fail(f"Detail container NOT found after {time.time()-t0:.1f}s: {DETAIL_CONTAINER}")
        info("Page URL: " + page.url)
        info("Page title: " + await page.title())
        # Try to find what IS on the page
        containers = await page.evaluate("""
            () => Array.from(document.querySelectorAll('[class*="billboard"]'))
                       .map(el => el.className)
                       .slice(0, 10)
        """)
        info("Elements with 'billboard' in class: " + json.dumps(containers))
        return

    # Test each inner selector individually
    info("--- Testing inner selectors ---")
    selectors = {
        "description container": ".cdmx-billboard-page-event-description-container",
        "description paragraphs": ".cdmx-billboard-page-event-description-container p",
        "info container": ".cdmx-billboard-page-event-info-container",
        "info bordered": ".cdmx-billboard-page-event-info-container-bordered",
        "info list items": ".cdmx-billboard-page-event-info-container-bordered ul li",
        "schedule container": ".cdmx-billboard-page-event-schedule-container",
        "schedule date label": "#cdmx-billboard-current-date-label",
        "schedule hour label": "#cdmx-billboard-current-hour-label",
        "location container": ".cdmx-billboard-page-event-location-container",
        "banner image": ".container-fluid.cdmx-billboard-page-event-banner-image",
        "title wrapper": ".cdmx-billboard-page-event-banner-image-titles",
        "evento h1": ".cdmx-billboard-page-event-banner-image-titles h1",
        "recinto h2": ".cdmx-billboard-page-event-banner-image-titles h2",
    }

    for label, sel in selectors.items():
        el = await page.query_selector(sel)
        if el:
            try:
                text = (await el.inner_text()).strip()[:80].replace('\n', ' ')
                ok(f"{label}: '{text}'")
            except Exception:
                ok(f"{label}: found (couldn't get text)")
        else:
            fail(f"{label}: NOT FOUND  [{sel}]")

    # Run the actual scrape_inner_page logic inline
    info("--- Running full scrape_inner_page evaluate ---")
    data = await page.evaluate("""
        () => {
            const wrapper = document.querySelector('.cdmx-billboard-generic-page-container');
            if (!wrapper) return {error: 'wrapper not found'};
            const d = {};

            const desc = wrapper.querySelector('.cdmx-billboard-page-event-description-container');
            d.description = desc ? Array.from(desc.querySelectorAll("p"), p => p.innerText.trim()).filter(Boolean) : null;

            const info = wrapper.querySelector('.cdmx-billboard-page-event-info-container');
            if (info) {
                const bordered = info.querySelector('.cdmx-billboard-page-event-info-container-bordered');
                d.info = bordered ? Array.from(bordered.querySelectorAll("ul li"), li => li.innerText.trim()).filter(Boolean) : null;
            } else { d.info = null; }

            const sched = wrapper.querySelector('.cdmx-billboard-page-event-schedule-container');
            d.schedule = sched ? {
                date: sched.querySelector('#cdmx-billboard-current-date-label')?.innerText.trim() || null,
                hour: sched.querySelector('#cdmx-billboard-current-hour-label')?.innerText.trim() || null
            } : null;

            const loc = wrapper.querySelector('.cdmx-billboard-page-event-location-container');
            d.location = loc ? loc.querySelector("span")?.innerText.trim() : null;

            const banner = document.querySelector('.container-fluid.cdmx-billboard-page-event-banner-image');
            if (banner) {
                const style = banner.getAttribute("style") || "";
                const match = style.match(/url\\(['"]?(.*?)['"]?\\)/);
                d.banner_url = match ? match[1] : null;
            } else { d.banner_url = null; }

            const titleWrapper = document.querySelector('.cdmx-billboard-page-event-banner-image-titles');
            d.evento = titleWrapper?.querySelector('h1')?.innerText.trim() || null;
            d.recinto = titleWrapper?.querySelector('h2')?.innerText.trim() || null;

            return d;
        }
    """)
    info("Scraped data:")
    print(json.dumps(data, ensure_ascii=False, indent=4))

    # Count nulls
    if isinstance(data, dict) and "error" not in data:
        nulls = [k for k, v in data.items() if v is None]
        empties = [k for k, v in data.items() if v == [] or v == ""]
        if nulls:
            fail(f"Fields still None: {nulls}")
        if empties:
            fail(f"Fields empty: {empties}")
        if not nulls and not empties:
            ok("All fields populated!")


async def main():
    print(f"\nDiagnostic scraper test — page={PAGE_NUMBER}, max_cards={MAX_CARDS}")
    print(f"Target: {BASE_URL}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context()

        # Block images/media/fonts to speed up
        async def route_handler(route):
            if route.request.resource_type in {"image", "media", "font"}:
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", route_handler)

        page = await context.new_page()
        page.set_default_navigation_timeout(30_000)
        page.set_default_timeout(20_000)

        try:
            await check_page_numbers(page)
            card_count = await check_card_listing(page)
            if card_count > 0:
                for i in range(min(MAX_CARDS, card_count)):
                    await check_card_detail(page, i)
        finally:
            await browser.close()

    section("DONE")


asyncio.run(main())
