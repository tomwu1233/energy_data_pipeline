# Energy Data Pipeline

The data engineering repository for Swedish Energy Demand Forecasting. V1 fetches the previous 24 completed hours of ENTSO-E SE3 Actual Total Load and stores each complete API response as one raw XML record in the Databricks Bronze Delta table. It does not parse or aggregate the response.

## Layout

- `pipeline/entsoe_bronze.py`: reusable window, API, record-building, and idempotent Bronze-write logic.
- `jobs/run_entsoe_bronze.py`: Databricks Jobs entry point; retrieves the API token from Databricks Secrets.
- `tests/test_entsoe_bronze.py`: deterministic unit tests for the V1 ingestion behavior.

## Databricks Setup

Run the job as a Python task on a Databricks Runtime with Spark and Delta Lake available. Set these task environment variables:

- `ENTSOE_SECRET_SCOPE`: Databricks secret scope containing the ENTSO-E token.
- `ENTSOE_SECRET_KEY`: key for that token in the scope.
- `BRONZE_TABLE` (optional): defaults to `workspace.energy_bronze.entsoe_raw`.
- `INGESTION_ID` (optional): supply the original ID when manually resuming an ingestion outside its original Databricks job run.

Provision the catalog, schema, and table before the first run:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.energy_bronze;

CREATE TABLE IF NOT EXISTS workspace.energy_bronze.entsoe_raw (
  ingestion_id STRING NOT NULL,
  source STRING NOT NULL,
  area STRING NOT NULL,
  request_start TIMESTAMP NOT NULL,
  request_end TIMESTAMP NOT NULL,
  request_started_at TIMESTAMP NOT NULL,
  response_received_at TIMESTAMP NOT NULL,
  written_at TIMESTAMP NOT NULL,
  http_status INT NOT NULL,
  raw_xml STRING NOT NULL
)
USING DELTA;
```

The recommended schedule is hourly. Each run requests the rolling 24 hours ending at the most recent completed UTC hour. Job retries use a deterministic ingestion ID derived from the Databricks job run ID; the Delta merge inserts only when that ID is absent.

## Local Tests

Install the dependencies and run:

```text
uv sync
uv run pytest
```