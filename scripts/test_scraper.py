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


async def capture_api_requests(context, page_number: int):
    """TEST 0: Intercept /api/v1/internal/events/* calls using page.route() for reliable req+resp capture."""
    section(f"TEST 0 — Intercept internal API calls (page {page_number})")

    page = await context.new_page()
    url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
    info(f"Loading {url} ...")

    # Capture request body via passive listener (does not block XHR)
    req_bodies = {}
    page.on("request", lambda r: req_bodies.update(
        {r.url: r.post_data or ""}) if "/api/v1/internal/events/" in r.url else None
    )

    # expect_response waits for the API response without blocking it
    try:
        async with page.expect_response(
            lambda r: "/api/v1/internal/events/" in r.url, timeout=20_000
        ) as resp_info:
            await page.goto(url, wait_until="domcontentloaded")
            await page.evaluate("""
                () => new Promise(resolve => {
                    let total = 0;
                    const t = setInterval(() => {
                        window.scrollBy(0, 500); total += 500;
                        if (total >= document.body.scrollHeight) { clearInterval(t); resolve(); }
                    }, 200);
                })
            """)

        response = await resp_info.value
        req_body = req_bodies.get(response.url, "")
        try:
            resp_json = await response.json()
            resp_snippet = json.dumps(resp_json, ensure_ascii=False, indent=2)[:3000]
        except Exception:
            resp_snippet = (await response.text())[:800]

        ok(f"Captured API call: [{response.request.method}] {response.url}")
        print(f"\n  Request body ({len(req_body)} bytes):\n{req_body}")
        print(f"\n  Response HTTP {response.status}:\n{resp_snippet}")

    except PlaywrightTimeoutError:
        fail("No /api/v1/internal/events/ response captured within 20s")
    finally:
        await page.close()


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
    await page.wait_for_timeout(3000)  # extra wait for JS render

    # Check paginator container
    container = await page.query_selector("#cdmx-billboard-event-paginator")
    if container:
        ok("Paginator container found: #cdmx-billboard-event-paginator")
    else:
        fail("Paginator container NOT found: #cdmx-billboard-event-paginator")

    # Wait explicitly for page buttons
    try:
        await page.wait_for_selector("#cdmx-billboard-event-paginator li.page.btn", timeout=15_000)
        ok("Page buttons appeared after explicit wait")
    except PlaywrightTimeoutError:
        fail("Page buttons NEVER appeared within 15s after explicit wait")
        # Dump paginator innerHTML to see actual DOM
        inner = await page.evaluate("""
            () => {
                const el = document.querySelector('#cdmx-billboard-event-paginator');
                return el ? el.innerHTML.trim().slice(0, 800) : 'not found';
            }
        """)
        info("Paginator innerHTML: " + inner)

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

    card_locator_selector = CARD_SELECTOR
    card = page.locator(card_locator_selector).nth(card_index)
    await card.scroll_into_view_if_needed()

    # Inspect card data attributes (site uses data-event-slug, NOT <a href>)
    card_el = await card.element_handle()
    card_html = await page.evaluate("(el) => el.outerHTML.slice(0, 500)", card_el)
    info(f"Card {card_index} outerHTML snippet:\n    {card_html}")

    event_slug = await card.get_attribute("data-event-slug")
    modal_id   = await card.get_attribute("data-modal-id")
    info(f"data-event-slug='{event_slug}'  data-modal-id='{modal_id}'")

    if not event_slug:
        fail(f"Card {card_index} has no data-event-slug — cannot build detail URL")
        return

    # Dismiss cookie/consent banners if present
    for dismiss_sel in [
        "button[id*='accept']", "button[class*='accept']", "button[class*='cookie']",
        "#onetrust-accept-btn-handler", ".cookie-accept", "[aria-label*='Accept']",
    ]:
        el = await page.query_selector(dismiss_sel)
        if el:
            await el.click()
            info(f"Dismissed banner: {dismiss_sel}")
            await page.wait_for_timeout(500)
            break

    # Check for any full-page overlays covering the card
    overlay_info = await page.evaluate("""
        () => {
            const overlays = Array.from(document.querySelectorAll(
                '[style*="position: fixed"], [style*="position:fixed"], .modal, .overlay, .popup'
            )).filter(el => {
                const r = el.getBoundingClientRect();
                return r.width > 200 && r.height > 200;
            });
            return overlays.map(el => el.tagName + '#' + el.id + '.' + el.className.slice(0,60));
        }
    """)
    if overlay_info:
        info(f"Overlays/modals detected: {overlay_info}")
    else:
        ok("No large overlays detected")

    # Check what element Playwright sees at the card's center point
    bbox = await card.bounding_box()
    if bbox:
        cx = bbox['x'] + bbox['width'] / 2
        cy = bbox['y'] + bbox['height'] / 2
        el_at_center = await page.evaluate(
            f"() => {{ const el = document.elementFromPoint({cx:.1f}, {cy:.1f}); "
            f"return el ? el.tagName + ' | ' + el.className.slice(0,100) : 'none'; }}"
        )
        info(f"Element at card center ({cx:.0f},{cy:.0f}): {el_at_center}")
    else:
        info("Card has no bounding box (not in viewport)")

    # Check for sticky/fixed headers covering the card
    fixed_elements = await page.evaluate("""
        () => Array.from(document.querySelectorAll('*')).filter(el => {
            const s = window.getComputedStyle(el);
            return (s.position === 'fixed' || s.position === 'sticky')
                   && el.getBoundingClientRect().height > 20;
        }).map(el => ({
            tag: el.tagName,
            cls: el.className.slice(0, 80),
            rect: el.getBoundingClientRect()
        }))
    """)
    if fixed_elements:
        info(f"Fixed/sticky elements ({len(fixed_elements)}):")
        for fe in fixed_elements[:5]:
            r = fe['rect']
            print(f"    {fe['tag']}.{fe['cls'][:60]}  bottom={r['bottom']:.0f}px")
    else:
        ok("No fixed/sticky elements found")

    # Dismiss SweetAlert2 popup if present (it blocks card.click() and takes ~15s to auto-dismiss)
    if await page.query_selector(".swal2-container"):
        await page.keyboard.press("Escape")
        ok("Dismissing swal2 popup via Escape")
        try:
            await page.wait_for_selector(".swal2-container", state="hidden", timeout=3_000)
            ok("swal2 popup dismissed")
        except PlaywrightTimeoutError:
            fail("swal2 popup did not dismiss within 3s")
    else:
        ok("No swal2 popup present")

    # Try real Playwright click (generates isTrusted=true events)
    info("Trying real Playwright click (no force) ...")
    url_before = page.url
    t0 = time.time()
    try:
        await card.click(timeout=15_000)
        ok(f"Real click succeeded in {time.time()-t0:.1f}s")
    except PlaywrightTimeoutError:
        fail(f"Real click timed out after {time.time()-t0:.1f}s")
        # Last resort: dispatch trusted click via CDP
        info("Dispatching click via CDP dispatchMouseEvent ...")
        if bbox:
            cdp = await context.new_cdp_session(page)
            await cdp.send("Input.dispatchMouseEvent", {
                "type": "mousePressed", "x": cx, "y": cy,
                "button": "left", "clickCount": 1
            })
            await cdp.send("Input.dispatchMouseEvent", {
                "type": "mouseReleased", "x": cx, "y": cy,
                "button": "left", "clickCount": 1
            })
            await page.wait_for_timeout(500)
            ok(f"CDP click dispatched at ({cx:.0f},{cy:.0f})")
        else:
            fail("No bounding box, cannot dispatch CDP click")
            return
    except Exception as e:
        fail(f"Real click raised: {e}")
        return

    # Capture requests + DOM scan after click
    requests_made = []
    page.on("request", lambda req: requests_made.append((req.resource_type, req.url)))
    await page.wait_for_timeout(5_000)
    info(f"Requests fired after click ({len(requests_made)} total):")
    for rtype, rurl in requests_made[:20]:
        print(f"    [{rtype:10s}] {rurl}")

    new_elements = await page.evaluate("""
        () => [
            '.cdmx-billboard-generic-page-container',
            '[class*="modal"]',
            '[class*="event-detail"]',
            '[class*="billboard-page"]',
        ].map(sel => ({ sel, found: !!document.querySelector(sel),
            html: document.querySelector(sel)?.outerHTML.slice(0,200)||null }))
    """)
    info("DOM scan after click:")
    for entry in new_elements:
        print(f"    {'[OK]  ' if entry['found'] else '[FAIL]'} {entry['sel']}")
        if entry["html"]: print(f"       {entry['html']}")

    if page.url != url_before:
        ok(f"URL changed to: {page.url}")
    else:
        info(f"URL unchanged after click: {page.url}")

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
        # No resource blocking — we need to observe ALL requests the JS makes
        # (including XHR/fetch for modal content)
        pass

        page = await context.new_page()
        page.set_default_navigation_timeout(30_000)
        page.set_default_timeout(20_000)

        try:
            await capture_api_requests(context, PAGE_NUMBER)
            await check_page_numbers(page)
            card_count = await check_card_listing(page)
            if card_count > 0:
                for i in range(min(MAX_CARDS, card_count)):
                    await check_card_detail(page, i)
        finally:
            await browser.close()

    section("DONE")


asyncio.run(main())
