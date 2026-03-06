from playwright.sync_api import sync_playwright
import json
import time

RETRYABLE_ERROR_FRAGMENTS = (
    "Execution context was destroyed",
    "Navigation failed",
    "Target page, context or browser has been closed",
    "Timeout",
)


def _is_retryable_error(exc):
    err_text = str(exc)
    return any(fragment in err_text for fragment in RETRYABLE_ERROR_FRAGMENTS)


def _log_event(message, **fields):
    payload = {"message": message, **fields}
    print(json.dumps(payload, ensure_ascii=False))


def _emit_metric(metric_name, value, unit="Count", dimensions=None):
    dims = dimensions or {"Function": "CulturaPageCheck"}
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


def _retry_sync(
    fn,
    retries=3,
    base_delay=0.6,
    retryable_predicate=None,
    metrics=None,
    operation="unknown_operation",
):

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
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
            time.sleep(base_delay * (2 ** (attempt - 1)))
    raise last_exc

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


def _elapsed_ms(started_at):
    return int((time.time() - started_at) * 1000)


def handler(event, context):
    """AWS Lambda handler to detect the last page number on cartelera.cdmx.gob.mx and return the page array."""
    started_at = time.time()
    metrics = {"retry_attempts": 0}

    _log_event("handler_started", event=str(event))

    _log_event("playwright_init_start", elapsed_ms=_elapsed_ms(started_at))
    with sync_playwright() as p:
        _log_event("playwright_init_done", elapsed_ms=_elapsed_ms(started_at))

        _log_event("browser_launch_start", elapsed_ms=_elapsed_ms(started_at))
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
        _log_event("browser_launch_done", elapsed_ms=_elapsed_ms(started_at))

        context = browser.new_context()
        context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )
        page = context.new_page()
        page.set_default_navigation_timeout(30_000)
        page.set_default_timeout(20_000)
        _log_event("page_created", elapsed_ms=_elapsed_ms(started_at))

        # Go to the main search page
        _log_event("navigation_start", url="https://cartelera.cdmx.gob.mx/busqueda", elapsed_ms=_elapsed_ms(started_at))
        _retry_sync(
            lambda: page.goto("https://cartelera.cdmx.gob.mx/busqueda", wait_until="domcontentloaded"),
            retries=3,
            base_delay=0.8,
            retryable_predicate=_is_retryable_error,
            metrics=metrics,
            operation="pagecheck_goto",
        )
        _log_event("navigation_done", url=page.url, elapsed_ms=_elapsed_ms(started_at))

        _log_event("scroll_start", elapsed_ms=_elapsed_ms(started_at))
        scroll_to_bottom(page)
        _log_event("scroll_done", elapsed_ms=_elapsed_ms(started_at))

        _log_event("wait_paginator_start", selector="#cdmx-billboard-event-paginator li.page.btn", elapsed_ms=_elapsed_ms(started_at))
        page.wait_for_selector("#cdmx-billboard-event-paginator li.page.btn", timeout=20_000)
        _log_event("wait_paginator_done", elapsed_ms=_elapsed_ms(started_at))

        # Locate the last page button in the paginator
        _log_event("query_last_page_start", elapsed_ms=_elapsed_ms(started_at))
        paginator_last = page.query_selector(
            "#cdmx-billboard-event-paginator li.page.btn[jp-role='last']"
        )
        last_page_attr = paginator_last.get_attribute("jp-data") if paginator_last else None

        if last_page_attr:
            last_page = int(last_page_attr)
        else:
            # No "last" button — check if any page buttons exist at all
            page_buttons = page.query_selector_all("#cdmx-billboard-event-paginator li.page.btn")
            btn_count = len(page_buttons)
            _log_event("paginator_no_last_btn", page_button_count=btn_count, elapsed_ms=_elapsed_ms(started_at))
            if btn_count == 0:
                raise RuntimeError(
                    "Paginator container found but zero page buttons rendered — page load incomplete"
                )
            # Buttons exist but no 'last' button means legitimately 1 page
            last_page = 1

        _log_event(
            "query_last_page_done",
            paginator_last_found=paginator_last is not None,
            jp_data_attr=last_page_attr,
            last_page=last_page,
            elapsed_ms=_elapsed_ms(started_at),
        )

        _log_event("browser_close_start", elapsed_ms=_elapsed_ms(started_at))
        browser.close()
        _log_event("browser_close_done", elapsed_ms=_elapsed_ms(started_at))

    # Create an array of page numbers from 1 to last_page
    page_numbers = list(range(1, last_page + 1))
    duration_ms = int((time.time() - started_at) * 1000)

    _emit_metric("PageCheckDurationMs", duration_ms, unit="Milliseconds")
    _emit_metric("PageCheckLastPage", last_page)
    _emit_metric("PageCheckRetryAttempts", metrics["retry_attempts"])
    _log_event(
        "page_check_completed",
        last_page=last_page,
        page_count=len(page_numbers),
        retry_attempts=metrics["retry_attempts"],
        duration_ms=duration_ms,
    )

    return {
        "statusCode": 200,
        "body": {
            "last_page": last_page,
            "page_numbers": page_numbers
        }
    }
