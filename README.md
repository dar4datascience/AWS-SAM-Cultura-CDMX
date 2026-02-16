# AWS-SAM-Cultura-CDMX

Automated data pipeline for CDMX cultural events:

1. **Scrapes** https://cartelera.cdmx.gob.mx with Playwright on AWS Lambda
2. **Orchestrates** the full run with AWS Step Functions
3. **Stores** raw JSON + curated Parquet in S3
4. **Publishes** the final Parquet file back to this repository (`/data/...`)

---

## Current state (high-level)

The infrastructure is defined with **AWS SAM** under `cultura-cdmx-sam/` and deployed as stack:

- `cultura-cartelera-cdmx` in region `mx-central-1`

Execution flow:

1. Generate a `snapshot_date` (`YYYYMMDD`)
2. Detect how many result pages currently exist in cartelera
3. Map over all pages with parallel Lambda executions (max concurrency 5)
4. Save one JSON per page into S3
5. Merge all JSON files into one compressed Parquet with DuckDB
6. Upload that Parquet to this repo via GitHub API

---

## 1) Scraping logic ("scrapping")

### A. Page discovery Lambda (`cultura_check_page`)

- Runtime style: **container image** with Playwright
- Entry point: `cultura-cdmx-sam/cultura_check_page/app.py`
- Purpose:
  - Opens `https://cartelera.cdmx.gob.mx/busqueda`
  - Scrolls to trigger lazy content
  - Reads paginator last page (`jp-role='last'`, `jp-data`)
  - Returns all page numbers `[1..last_page]`

This Lambda answers: **"How many pages must we scrape today?"**

### B. Card scraper Lambda (`playwright_card_scrape`)

- Runtime style: **container image** with Playwright
- Entry point: `cultura-cdmx-sam/playwright_card_scrape/app.py`
- Triggered once per page from Step Functions `Map`

For each search-results page:

1. Opens `https://cartelera.cdmx.gob.mx/busqueda?tipo=ALL&pagina=<n>`
2. Scrolls to force lazy-loaded cards
3. Iterates card-by-card
4. Clicks each card, extracts detail page fields, then returns to listing

Extracted fields include:

- `evento`
- `recinto`
- `description` (paragraph array)
- `info` (list array)
- `schedule` (`date`, `hour`)
- `location`
- `banner_url`
- plus metadata: `page_number`, `card_index`, `detail_url`

Raw output is written to S3 as:

```text
snapshot_date/<YYYYMMDD>/events_page_<n>.json
```

### C. Snapshot date Lambda (`generate_snapshot_date`)

- Entry point: `cultura-cdmx-sam/generate_snapshot_date/app.py`
- Returns UTC date in `YYYYMMDD`
- Guarantees each run is namespaced under a deterministic daily snapshot

### D. DuckDB consolidation Lambda (`duckdb_handler`)

- Runtime style: zip-based Python Lambda + DuckDB layer
- Entry point: `cultura-cdmx-sam/duckdb_handler/app.py`

Responsibilities:

1. Reads all JSON files for one snapshot from S3
2. Uses DuckDB `read_json_auto(...)`
3. Writes a ZSTD-compressed Parquet back to S3:

```text
database/scraped_data_<YYYYMMDD>.parquet
```

4. Downloads that Parquet into `/tmp`
5. Retrieves GitHub token from AWS Secrets Manager
6. Uploads gzipped + base64 content to repo path:

```text
data/scraped_data_cultura_cartelera_cdmx.parquet
```

---

## 2) Infrastructure logic and deployment with SAM

Main infra file:

- `cultura-cdmx-sam/template.yml`

### Deployed resources (current)

- **Lambdas**
  - `CulturaPageCheck` (Playwright, image)
  - `PlaywrightCardScrapper` (Playwright, image)
  - `GenerateSnapshotDateLambda` (Python)
  - `DuckDBFunction` (Python + layer)
- **Layer**
  - `DuckDBLayer`
- **Storage**
  - `CulturaBucket` (private, lifecycle expiration 30 days)
- **Orchestration**
  - `CulturaScrapeStateMachine` (Step Functions)
- **Scheduling**
  - `CulturaScrapeScheduleRule` (EventBridge cron)
- **IAM roles/policies** for Lambda, Step Functions, EventBridge invoke, S3 access, Secrets Manager read

### Step Functions orchestration

State machine sequence:

1. `GenerateSnapshotDate`
2. `GetPageNumbers`
3. `ScrapePagesMap` (`MaxConcurrency: 5`)
4. `DuckDBStep`

This gives a good balance between scraping speed and Lambda resource usage.

### Schedule (automated runs)

EventBridge cron in template:

```text
cron(0 12 ? * 2-6 *)
```

- 12:00 UTC
- Monday to Friday
- Intended as morning schedule for Mexico City

### SAM deployment config

`cultura-cdmx-sam/samconfig.yaml` defines:

- stack name: `cultura-cartelera-cdmx`
- region: `mx-central-1`
- capabilities: `CAPABILITY_IAM`

---

## Deploy / update infrastructure

From `cultura-cdmx-sam/`:

```bash
sam build
sam deploy --config-file samconfig.yaml --resolve-image-repos
```

Notes:

- `--resolve-image-repos` is required for the Playwright container Lambdas.
- First deploy may ask for guided confirmations depending on local SAM CLI settings.

---

## Manual testing (useful during development)

### Invoke card scraper Lambda directly

```bash
aws lambda invoke \
  --function-name <PlaywrightCardScrapper-physical-name> \
  --cli-binary-format raw-in-base64-out \
  --payload '{"page_number": 4, "snapshot_date": "20260216"}' \
  response.json
```

### Invoke DuckDB Lambda directly

```bash
aws lambda invoke \
  --function-name <DuckDBFunction-physical-name> \
  --cli-binary-format raw-in-base64-out \
  --payload '{"snapshot_date": "20260216"}' \
  response.json
```

### Start full state machine execution

```bash
aws stepfunctions start-execution \
  --state-machine-arn <CulturaScrapeStateMachine-arn>
```

---

## Repository structure

```text
.
├── cultura-cdmx-sam/
│   ├── template.yml
│   ├── samconfig.yaml
│   ├── cultura_check_page/        # Detect paginator last page
│   ├── playwright_card_scrape/    # Scrape each results page
│   ├── generate_snapshot_date/    # Create snapshot_date
│   ├── duckdb_handler/            # Merge JSON -> Parquet + upload to GitHub
│   └── layers/duckdb/             # DuckDB Lambda layer
├── data/
│   └── scraped_data_cultura_cartelera_cdmx.parquet
└── .github/workflows/
    └── deploy-quarto.yaml         # Quarto/GitHub Pages site deploy
```

---

## Operational notes

- The scraper relies on current CSS selectors from cartelera.cdmx.gob.mx; if the site changes markup, selectors may need updates.
- `DuckDBFunction` currently depends on `GITHUB_SECRET_ARN` for token retrieval. Keep this secret managed in AWS Secrets Manager (never hardcode tokens in source).
- S3 lifecycle is configured to remove old objects after 30 days to limit storage growth.

---

## Next recommended improvements

1. Add CloudWatch alarms for failed state-machine runs.
2. Add retry/catch policies per Step Functions task for better resilience.
3. Publish date-partitioned Parquet snapshots in-repo (instead of overwriting one file) if historical tracking is required.
