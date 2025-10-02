from playwright.sync_api import Playwright, sync_playwright, expect

def run(playwright: Playwright):
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    # Go to main page
    page.goto("https://cartelera.cdmx.gob.mx/busqueda")

    # Wait for "eventos para ti" and click it
    expect(page.locator(".cdmx-billboard-home-top-container-title-container-titles")).to_be_visible()
    page.get_by_text("eventos para ti").click()
    expect(page.get_by_text("eventos para ti")).to_be_visible()

    # Scroll to bottom to load lazy content
    page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)

    # Grab the first card container
    first_card = page.locator(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    ).first

    # Print inner HTML for debugging
    print(f"First card HTML:\n{first_card.inner_html()}\n{'-'*50}")

    # Click the card itself
    first_card.click()
    page.wait_for_timeout(1500)  # wait for navigation

    print(f"Currently on detail page: {page.url}")

    # Click the red "Regresar a búsqueda" button
    back_button = page.locator("#cdmx-billboard-return-home-button")
    expect(back_button).to_be_visible()
    back_button.click()
    page.wait_for_timeout(1500)

    print(f"Back to main page: {page.url}")

    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
