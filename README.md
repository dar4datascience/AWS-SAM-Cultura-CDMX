# AWS-SAM-Cultura-CDMX
AWS SAM Cultura CDMX

- bucket
- lamba scrape inner
- lamba open other lambas
- lambda integrate result
- lambda deliver data

```bash
sam build

sam deploy --config-file samconfig.yaml 

```

PENDING ADDING MORE DEBUGGING TO INCREASE PARSING SPEED PER PAGE

concurrency vs speed. should i just end up using glue?

```
{
  "errorMessage": "BrowserContext.new_page: Target page, context or browser has been closed",
  "errorType": "TargetClosedError",
  "requestId": "abc3f669-725c-4ff4-9a34-b8513e512091",
  "stackTrace": [
    "  File \"/function/app.py\", line 175, in handler\n    results = asyncio.run(run_scraper(page_number, max_concurrent=3))\n",
    "  File \"/usr/lib/python3.12/asyncio/runners.py\", line 194, in run\n    return runner.run(main)\n",
    "  File \"/usr/lib/python3.12/asyncio/runners.py\", line 118, in run\n    return self._loop.run_until_complete(task)\n",
    "  File \"/usr/lib/python3.12/asyncio/base_events.py\", line 687, in run_until_complete\n    return future.result()\n",
    "  File \"/function/app.py\", line 163, in run_scraper\n    results = await scrape_page_cards(browser, page_number, max_concurrent=max_concurrent)\n",
    "  File \"/function/app.py\", line 137, in scrape_page_cards\n    page = await context.new_page()\n",
    "  File \"/function/playwright/async_api/_generated.py\", line 12793, in new_page\n    return mapping.from_impl(await self._impl_obj.new_page())\n",
    "  File \"/function/playwright/_impl/_browser_context.py\", line 335, in new_page\n    return from_channel(await self._channel.send(\"newPage\", None))\n",
    "  File \"/function/playwright/_impl/_connection.py\", line 69, in send\n    return await self._connection.wrap_api_call(\n",
    "  File \"/function/playwright/_impl/_connection.py\", line 558, in wrap_api_call\n    raise rewrite_error(error, f\"{parsed_st['apiName']}: {error}\") from None\n"
  ]
}
```


testng sequantial scrape for cards in page