import asyncio
import json
import os
import time
import boto3
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# S3 client outside the handler for reuse
s3 = boto3.client("s3")

RETRYABLE_ERROR_FRAGMENTS = (
    "Execution context was destroyed",
    "Navigation failed",
    "Target page, context or browser has been closed",
    "Timeout",
)


def _empty_event_payload(detail_url=None):
    return {
        "detail_url": detail_url,
        "description": None,
        "info": None,
        "schedule": None,
        "location": None,
        "banner_url": None,
        "evento": None,
        "recinto": None,
    }


def _is_retryable_error(exc):
    err_text = str(exc)
    return any(fragment in err_text for fragment in RETRYABLE_ERROR_FRAGMENTS)


def _log_event(message, **fields):
    payload = {"message": message, **fields}
    print(json.dumps(payload, ensure_ascii=False))


def _emit_metric(metric_name, value, unit="Count", dimensions=None):
    dims = dimensions or {"Function": "PlaywrightCardScrapper"}
    metric_event = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "CulturaScraper",
                    "Dimensions": [list(dims.keys())],
                    "Metrics": [{"Name": metric_name, "Unit": unit}],
                }
            ],
        },
        metric_name: value,
        **dims,
    }
    print(json.dumps(metric_event, ensure_ascii=False))


async def _retry_async(
    coro_factory,
    retries=3,
    base_delay=0.6,
    retryable_predicate=None,
    metrics=None,
    operation="unknown_operation",
):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return await coro_factory()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if retryable_predicate and not retryable_predicate(exc):
                raise
            if attempt == retries:
                raise
            if metrics is not None:
                metrics["retry_attempts"] = metrics.get("retry_attempts", 0) + 1
            _log_event(
                "retry_scheduled",
                operation=operation,
                attempt=attempt,
                max_attempts=retries,
                error=str(exc),
            )
            await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
    raise last_exc


async def scroll_to_bottom(page, distance=500, timeout_ms=1000):
    """Scroll to the bottom incrementally to trigger lazy-loaded content."""
    await page.evaluate(f"""
        () => {{
            return new Promise(resolve => {{
                let totalHeight = 0;
                const distance = {distance};
                const timer = setInterval(() => {{
                    const scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    if (totalHeight >= scrollHeight) {{
                        clearInterval(timer);
                        resolve();
                    }}
                }}, 200);
            }});
        }}
    """)
    await page.wait_for_timeout(timeout_ms)

async def scrape_inner_page(page, retries=2):
    """Scrape event details (description, info, schedule, location, banner image, evento, recinto) from inner page with retries."""
    for attempt in range(1, retries + 1):
        try:
            data = await page.evaluate("""
                () => {
                    const wrapper = document.querySelector('.cdmx-billboard-generic-page-container');
                    if (!wrapper) return null;
                    const d = {};
                    
                    // Description
                    const desc = wrapper.querySelector('.cdmx-billboard-page-event-description-container');
                    d.description = desc ? Array.from(desc.querySelectorAll("p"), p => p.innerText.trim()).filter(Boolean) : null;

                    // Info
                    const info = wrapper.querySelector('.cdmx-billboard-page-event-info-container');
                    if (info) {
                        const bordered = info.querySelector('.cdmx-billboard-page-event-info-container-bordered');
                        d.info = bordered ? Array.from(bordered.querySelectorAll("ul li"), li => li.innerText.trim()).filter(Boolean) : null;
                    } else {
                        d.info = null;
                    }

                    // Schedule
                    const sched = wrapper.querySelector('.cdmx-billboard-page-event-schedule-container');
                    d.schedule = sched ? {
                        date: sched.querySelector('#cdmx-billboard-current-date-label')?.innerText.trim() || null,
                        hour: sched.querySelector('#cdmx-billboard-current-hour-label')?.innerText.trim() || null
                    } : null;

                    // Location
                    const loc = wrapper.querySelector('.cdmx-billboard-page-event-location-container');
                    d.location = loc ? loc.querySelector("span")?.innerText.trim() : null;

                    // Banner URL (from inline style)
                    const banner = document.querySelector('.container-fluid.cdmx-billboard-page-event-banner-image');
                    if (banner) {
                        const style = banner.getAttribute("style") || "";
                        const match = style.match(/url\\(['"]?(.*?)['"]?\\)/);
                        d.banner_url = match ? match[1] : null;
                    } else {
                        d.banner_url = null;
                    }

                    // Evento and Recinto
                    const titleWrapper = document.querySelector('.cdmx-billboard-page-event-banner-image-titles');
                    d.evento = titleWrapper?.querySelector('h1')?.innerText.trim() || null;
                    d.recinto = titleWrapper?.querySelector('h2')?.innerText.trim() || null;

                    return d;
                }
            """)
            return data or _empty_event_payload()
        except Exception as e:  # noqa: BLE001
            if _is_retryable_error(e) and attempt < retries:
                await asyncio.sleep(0.8 * attempt)
                continue
            print(f"Error scraping inner page: {e}")
            return _empty_event_payload()
    return _empty_event_payload()

async def scrape_page_sequential(browser, page_number: int):
    """Scrape all cards sequentially on a page using a single browser context."""
    started_at = time.time()
    context = await browser.new_context()
    results = []
    metrics = {
        "retry_attempts": 0,
        "cards_failed": 0,
    }

    async def route_handler(route):
        if route.request.resource_type in {"image", "media", "font"}:
            await route.abort()
            return
        await route.continue_()

    await context.route(
        "**/*",
        route_handler,
    )

    # Retry opening page
    page = await _retry_async(
        lambda: context.new_page(),
        retries=3,
        base_delay=0.7,
        retryable_predicate=_is_retryable_error,
        metrics=metrics,
        operation="new_page",
    )

    page.set_default_navigation_timeout(60_000)
    page.set_default_timeout(30_000)

    url = f"https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina={page_number}"
    await _retry_async(
        lambda: page.goto(url, wait_until="domcontentloaded"),
        retries=3,
        base_delay=0.8,
        retryable_predicate=_is_retryable_error,
        metrics=metrics,
        operation="initial_goto",
    )
    await scroll_to_bottom(page)
    await page.wait_for_selector(
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container",
        timeout=25_000,
    )

    card_locator_selector = (
        "#cdmx-billboard-tab-event-list .cdmx-billboard-event-result-list-item-container"
    )
    card_count = await page.locator(card_locator_selector).count()

    for i in range(card_count):
        try:
            card = page.locator(card_locator_selector).nth(i)
            await card.scroll_into_view_if_needed()
            await card.click(timeout=15_000)

            # Wait for the detail container
            await page.wait_for_selector(".cdmx-billboard-generic-page-container", timeout=30_000)

            # Extract inner page data
            data = await scrape_inner_page(page)

            results.append({
                "page_number": page_number,
                "card_index": i,
                **_empty_event_payload(detail_url=page.url),
                **data,
            })

            # ✅ Go back to results page and re-scroll
            await _retry_async(
                lambda: page.go_back(wait_until="domcontentloaded"),
                retries=3,
                base_delay=0.7,
                retryable_predicate=_is_retryable_error,
                metrics=metrics,
                operation="go_back",
            )
            await scroll_to_bottom(page)

        except PlaywrightTimeoutError:
            print(f"Timeout scraping card {i} on page {page_number}")
            results.append({
                "page_number": page_number,
                "card_index": i,
                **_empty_event_payload(),
            })
            metrics["cards_failed"] += 1
            await _retry_async(
                lambda: page.goto(url, wait_until="domcontentloaded"),
                retries=2,
                base_delay=0.8,
                retryable_predicate=_is_retryable_error,
                metrics=metrics,
                operation="recovery_goto_timeout",
            )
            await scroll_to_bottom(page)
        except Exception as e:
            print(f"Failed to scrape card {i} on page {page_number}: {e}")
            results.append({
                "page_number": page_number,
                "card_index": i,
                **_empty_event_payload(),
            })
            metrics["cards_failed"] += 1
            await _retry_async(
                lambda: page.goto(url, wait_until="domcontentloaded"),
                retries=2,
                base_delay=0.8,
                retryable_predicate=_is_retryable_error,
                metrics=metrics,
                operation="recovery_goto_exception",
            )
            await scroll_to_bottom(page)

    await context.close()

    duration_ms = int((time.time() - started_at) * 1000)
    _emit_metric("ScrapePageDurationMs", duration_ms, unit="Milliseconds")
    _emit_metric("ScrapePageCardCount", len(results))
    _emit_metric("ScrapePageCardFailures", metrics["cards_failed"])
    _emit_metric("ScrapeRetryAttempts", metrics["retry_attempts"])
    _log_event(
        "scrape_page_completed",
        page_number=page_number,
        cards_scraped=len(results),
        cards_failed=metrics["cards_failed"],
        retry_attempts=metrics["retry_attempts"],
        duration_ms=duration_ms,
    )

    return results


async def run_scraper(page_number: int):
    """Launch Playwright browser and scrape the page sequentially."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--single-process",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--remote-debugging-port=9222"
            ]
        )
        try:
            results = await scrape_page_sequential(browser, page_number)
        finally:
            await browser.close()
        return results


def handler(event, context):
    """AWS Lambda handler."""
    handler_started_at = time.time()
    # Handle if event is a plain int (from Step Functions Map) or dict
    if isinstance(event, int):
        page_number = event
    elif isinstance(event, str) and event.isdigit():
        page_number = int(event)
    elif isinstance(event, dict):
        page_number = int(event.get("page_number", 1))
    else:
        page_number = 1

    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        return {"statusCode": 500, "body": "Environment variable BUCKET_NAME not set."}

    results = asyncio.run(run_scraper(page_number))

    snapshot_date = event.get("snapshot_date") if isinstance(event, dict) else datetime.utcnow().strftime("%Y-%m-%d")
    s3_key = f"snapshot_date/{snapshot_date}/events_page_{page_number}.json"
    body_payload = json.dumps(results, ensure_ascii=False, separators=(",", ":"))

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=body_payload
    )

    handler_duration_ms = int((time.time() - handler_started_at) * 1000)
    _emit_metric("ScrapeHandlerDurationMs", handler_duration_ms, unit="Milliseconds")
    _emit_metric("ScrapeS3PayloadBytes", len(body_payload), unit="Bytes")
    _log_event(
        "scrape_handler_completed",
        page_number=page_number,
        snapshot_date=snapshot_date,
        s3_key=s3_key,
        events_count=len(results),
        payload_bytes=len(body_payload),
        duration_ms=handler_duration_ms,
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Scraped page {page_number} and saved to s3://{bucket_name}/{s3_key}"
        })
    }
