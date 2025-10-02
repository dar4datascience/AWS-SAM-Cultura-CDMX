from playwright.sync_api import sync_playwright

def scroll_to_bottom(page):
    """Scroll to the bottom of the page incrementally to trigger lazy-loaded content."""
    page.evaluate("""
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
    page.wait_for_timeout(1000)


def handler(event, context):
    """AWS Lambda handler to detect the last page number on cartelera.cdmx.gob.mx."""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process"
            ]
        )

        context = browser.new_context()
        page = context.new_page()

        # Go to the main search page
        page.goto("https://cartelera.cdmx.gob.mx/busqueda", wait_until="load")
        scroll_to_bottom(page)

        # Locate the last page button in the paginator
        paginator_last = page.query_selector(
            "#cdmx-billboard-event-paginator li.page.btn[jp-role='last']"
        )
        last_page_attr = paginator_last.get_attribute("jp-data") if paginator_last else None
        last_page = int(last_page_attr) if last_page_attr else 1

        browser.close()

    return {
        "statusCode": 200,
        "body": {"last_page": last_page}
    }
