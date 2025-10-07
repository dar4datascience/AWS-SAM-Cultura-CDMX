# AWS-SAM-Cultura-CDMX

PENDING COALESCING DATA INTO SINGLE PARQUET TO DELVIER SOMEWHERE PUBLICLY TO ALLOW DUCKDB CONNECTION
one last step? using glue or lambda? whats the total size of the folder?

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
