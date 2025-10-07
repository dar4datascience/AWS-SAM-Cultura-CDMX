# AWS-SAM-Cultura-CDMX

- Missing title of the event
- make quarto full page

```bash
sam build

sam deploy --config-file samconfig.yaml --resolve-image-repos

aws lambda invoke \
  --function-name cultura-cartelera-cdmx-PlaywrightCardScrapper-2CUqPBlSBXUq \
  --cli-binary-format raw-in-base64-out \
  --payload '{"page_number": 4}' \
  response.json

aws lambda invoke \
  --function-name cultura-cartelera-cdmx-DuckDBFunction-1paMbykWXdlb \
  --cli-binary-format raw-in-base64-out \
  --payload '{"snapshot_date": "20251002"}' \
  response.json

```
