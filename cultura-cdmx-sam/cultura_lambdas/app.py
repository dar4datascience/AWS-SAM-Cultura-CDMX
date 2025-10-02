from playwright.sync_api import sync_playwright

def handler(event, context):
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
        # Create a new context per invocation
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://cartelera.cdmx.gob.mx/busqueda+" \
        "")
        content = page.content()

        browser.close()

    return {"statusCode": 200, "body": content}
