# scraper_utils.py
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
                    if(totalHeight >= scrollHeight){
                        clearInterval(timer);
                        resolve();
                    }
                }, 200);
            });
        }
    """)
    await page.wait_for_timeout(1000)

async def scrape_inner_page(page):
    """Scrape all relevant inner page sections inside the generic container with error handling."""
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
                    try {{
                        const el = wrapper.querySelector('.' + cls);
                        data[cls] = el ? el.outerHTML : null;
                    }} catch(e) {{
                        console.warn("Failed section", cls, e);
                        data[cls] = null;
                    }}
                }});
                return data;
            }}
        """)
        if html_content:
            data = html_content
        else:
            # fallback if wrapper not found
            for cls in sections:
                data[cls] = None
    except Exception as e:
        print(f"Error scraping inner page: {e}")
        # ensure all sections exist in output even if scrape fails
        for cls in sections:
            data[cls] = None

    return data


async def scrape_page(page):
    """Scrape all cards on the current page, including their inner pages sequentially."""
    events = []
    cards = page.locator(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    )
    count = await cards.count()
    print(f"Found {count} cards on this page")

    for i in range(count):
        card = cards.nth(i)
        await card.scroll_into_view_if_needed()

        # Extract main card data
        card_data = await card.evaluate("""
            (el) => {
                const imageDiv = el.querySelector(".cdmx-billboard-event-result-list-item-image");
                let image_url = null;
                if (imageDiv) {
                    const bg = imageDiv.style.backgroundImage;
                    if (bg && bg.startsWith('url(')) image_url = bg.slice(5, -2);
                }
                const meta = el.querySelector(".col-7");
                let type = null, name = null, venue = null;
                if (meta) {
                    const typeEl = meta.querySelector(".cdmx-billboard-event-result-list-item-event-type");
                    const nameEl = meta.querySelector(".cdmx-billboard-event-result-list-item-event-name");
                    const venueEl = meta.querySelector(".cdmx-billboard-event-result-list-item-event-venue");
                    type = typeEl ? typeEl.innerText : null;
                    name = nameEl ? nameEl.innerText : null;
                    venue = venueEl ? venueEl.innerText : null;
                }
                return {image_url, type, name, venue};
            }
        """)

        # Click card and scrape inner page
        event_detail_url = None
        detail_data = {}
        try:
            await card.click()
            await page.wait_for_timeout(1500)
            event_detail_url = page.url
            detail_data = await scrape_inner_page(page)

            # Return to main page
            back_button = page.locator("#cdmx-billboard-return-home-button")
            await expect(back_button).to_be_visible()
            await back_button.click()
            await page.wait_for_timeout(1500)
        except Exception as e:
            print(f"Failed to scrape detail page for card {i}: {e}")

        events.append({
            "event_image_url": card_data.get("image_url"),
            "event_type": card_data.get("type"),
            "event_name": card_data.get("name"),
            "event_venue": card_data.get("venue"),
            "event_detail_url": event_detail_url,
            **detail_data
        })

    return events

async def navigate_to_site(page):
    """Navigate to the main search page and scroll to load events."""
    await page.goto("https://cartelera.cdmx.gob.mx/busqueda")
    title_locator = page.locator(".cdmx-billboard-home-top-container-title-container-titles")
    await expect(title_locator).to_be_visible()
    title_text = await title_locator.inner_text()
    #print(title_text)
    match = re.search(r"\d+", title_text)
    total_events = int(match.group(0)) if match else None
    #print(f"Detected total events: {total_events}")
    await page.get_by_text("eventos para ti").click()
    await expect(page.get_by_text("eventos para ti")).to_be_visible()
    await scroll_to_bottom(page)
    return total_events

async def go_to_next_page(page, current_page):
    """Click the next page in paginator if available."""
    try:
        next_button = page.locator(
            f'#cdmx-billboard-event-paginator li.page.btn:has-text("{current_page+1}")'
        )
        if await next_button.count() > 0:
            await next_button.click()
            await page.wait_for_timeout(1000)
            await scroll_to_bottom(page)
            return True
        last_button = page.locator("#cdmx-billboard-event-paginator li.page.btn[jp-role='last']")
        if await last_button.count() > 0:
            await last_button.click()
            await page.wait_for_timeout(1000)
            await scroll_to_bottom(page)
            return True
    except Exception:
        return False
    return False

async def save_data(data, filename="events.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
