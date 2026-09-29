# Energy Data Pipeline — Repository Bootstrap Specification

## 1. Goal

Create a new, independent Git repository for the **data engineering layer** of the Swedish Energy Demand Forecasting project.

Repository name:

`energy-data-pipeline`

This repository is separate from the existing machine-learning repository:

`Time-Series-Forecasting-for-Swedish-Energy-Demand`

Do **not** modify or move files from the existing ML repository.

The responsibility boundary is:

```text
energy-data-pipeline
    External APIs
        ↓
    Bronze
        ↓
    Silver
        ↓
    Trusted hourly datasets

-------------------------------- repository boundary

Time-Series-Forecasting-for-Swedish-Energy-Demand
        ↓
    Feature Engineering
        ↓
    Model
        ↓
    Predictions / Gold
        ↓
    Evaluation
```

The new repository owns **data ingestion, raw data persistence, cleaning, standardization, aggregation, data quality, and orchestration up to Silver**.

It does **not** own model training, forecasting features, model inference, or forecast evaluation.

---

## 2. Current V1 Scope

Do not build the entire future architecture yet.

The first implementation has exactly one goal:

> Every hour, retrieve the previous 24 hours of ENTSO-E SE3 Actual Total Load data up to the current UTC hour boundary and persist the complete raw API response to the Databricks Bronze layer.

For example, if a run occurs at:

```text
2026-09-29 16:37 UTC
```

the logical request window should be:

```text
request_start = 2026-09-28 16:00 UTC
request_end   = 2026-09-29 16:00 UTC
```

V1 does **not** implement:

- Silver transformation
- A03 expansion
- PT15M → hourly aggregation
- weather ingestion
- feature engineering
- model inference
- Gold tables
- dashboard logic

These will be added incrementally later.

---

## 3. Initial Repository Structure

Create only the files/directories currently needed:

```text
energy-data-pipeline/
│
├── pipeline/
│   ├── __init__.py
│   └── entsoe_bronze.py
│
├── jobs/
│   └── run_entsoe_bronze.py
│
├── tests/
│   └── test_entsoe_bronze.py
│
├── README.md
├── requirements.txt
└── .gitignore
```

Do not create empty future directories such as:

```text
silver/
gold/
weather/
dashboard/
features/
models/
```

Add them only when they become necessary.

---

## 4. Responsibilities

### `pipeline/entsoe_bronze.py`

Contains reusable ENTSO-E Bronze ingestion logic.

Responsibilities:

1. Calculate the request time window.
2. Request ENTSO-E Actual Total Load data.
3. Validate the HTTP response.
4. Construct the Bronze ingestion record.
5. Persist the raw response into the Databricks Delta Bronze table.

Keep functions small and independently testable where practical.

The module should not depend on Notebook variables or manual Notebook execution order.

---

### `jobs/run_entsoe_bronze.py`

This is the production entry point.

Responsibilities:

1. Read runtime configuration.
2. Retrieve the ENTSO-E API token securely from Databricks Secrets.
3. Generate one `ingestion_id` for the logical ingestion attempt.
4. Call functions from `pipeline.entsoe_bronze`.
5. Surface failures clearly so Databricks Jobs can detect a failed run.

Business/transformation logic should not be duplicated here.

Conceptually:

```text
Databricks Job
      ↓
run_entsoe_bronze.py
      ↓
pipeline.entsoe_bronze
      ↓
ENTSO-E API
      ↓
Bronze Delta Table
```

---

## 5. Bronze Storage

Target table:

```text
workspace.energy_bronze.entsoe_raw
```

Bronze stores the original ENTSO-E API response.

One successful independent API ingestion corresponds to one Bronze ingestion record.

Bronze should preserve the raw XML rather than parsing it into load intervals.

Recommended fields:

```text
ingestion_id
source
area
request_start
request_end
request_started_at
response_received_at
written_at
http_status
raw_xml
```

Use UTC for timestamps.

Do not use one generic `ingested_at` timestamp for all lifecycle events.

The three operational timestamps have different meanings:

```text
request_started_at
    = when the API request started

response_received_at
    = when the API response was received

written_at
    = when the Bronze record was persisted
```

---

## 6. Ingestion Identity and Retry Rule

Distinguish between an independent ingestion run and a retry of the same logical ingestion.

### Independent runs

Two separately scheduled executions should have different `ingestion_id` values, even if their 24-hour request windows overlap.

Example:

```text
14:00 scheduled run → ingestion_id A
15:00 scheduled run → ingestion_id B
```

Both raw responses should be retained.

### Retry

If writing the Bronze record fails and the same logical ingestion is retried, reuse the original:

```text
ingestion_id
```

Do not generate a new ID merely because persistence is being retried.

The Bronze write should therefore be designed to avoid creating duplicate records for the same `ingestion_id`.

Do not assume that simple unconditional append is sufficient for retry-safe persistence.

---

## 7. Request Window

The production ingestion frequency will eventually be:

```text
every 1 hour
```

Each run retrieves a rolling:

```text
previous 24 hours
```

ending at the most recent completed UTC hour boundary.

Keep these two concepts separate:

```text
schedule frequency = 1 hour

request lookback window = 24 hours
```

The time-window calculation should be implemented as a pure/testable function.

Do not hide the current time deeply inside API logic.

Prefer allowing an explicit reference time to be passed so tests can be deterministic.

---

## 8. ENTSO-E Source Characteristics

Current source:

```text
Dataset: Actual Total Load
Area: SE3
ENTSO-E domain ID: 10Y1001A1001A46L
```

Observed source response currently uses:

```text
resolution = PT15M
curveType = A03
```

However, Bronze must not transform or aggregate these values.

The original XML response must be preserved.

The future Silver layer will be responsible for interpreting source resolution and A03 semantics.

Do not assume the source will always use PT15M.

---

## 9. Initial Backfill vs Incremental Ingestion

The rolling 24-hour request is for normal incremental operation.

It is **not sufficient for initial historical setup** because the forecasting model uses historical lags including:

```text
lag_168
lag_336
```

A separate historical backfill process will be designed later.

Do not implement backfill as part of the first V1 ingestion task.

---

## 10. Future Architecture

Do not implement this yet, but design current code so the repository can later evolve toward:

```text
External Sources
       │
       ├──────────────┐
       ↓              ↓
    ENTSO-E         Weather
       │              │
       ↓              ↓
┌─────────────────────────────┐
│ Bronze                      │
│ entsoe_raw                  │
│ weather_raw                 │
└──────────────┬──────────────┘
               ↓
┌─────────────────────────────┐
│ Silver                      │
│ load_hourly                 │
│ weather_hourly              │
│ data quality / lineage      │
└──────────────┬──────────────┘
               │
===============│================
 Repository boundary
               ↓
        ML repository
               ↓
       Feature Builder
               ↓
             Model
               ↓
             Gold
```

The canonical downstream load resolution for this project will be hourly.

If ENTSO-E supplies PT15M data, future Silver processing will parse and aggregate it to hourly data.

---

## 11. Silver Ownership

Future Silver responsibilities include:

- parsing raw ENTSO-E XML
- interpreting curve semantics such as A03
- timestamp normalization
- source-resolution handling
- deduplication
- revision handling
- data-quality checks
- coverage validation
- hourly aggregation
- lineage preservation

Feature engineering does **not** belong in this repository's Silver layer.

Examples that belong to the ML repository:

```text
lag_24
lag_168
lag_336
hour_of_day
day_of_week
model-specific weather features
```

Silver should expose trustworthy data facts; the ML repository decides how those facts become model features.

---

## 12. Weather

Weather will later have its own:

```text
Weather API
    ↓
Bronze weather_raw
    ↓
Silver weather_hourly
```

Weather forecast batches must eventually preserve forecast vintage/run information so the ML pipeline can reconstruct which weather forecast was available at prediction time.

Do not implement weather ingestion in the current task.

---

## 13. Databricks

Databricks is the execution and storage platform.

Current namespaces:

```text
workspace.energy_bronze
workspace.energy_silver
workspace.energy_gold
```

This repository owns:

```text
workspace.energy_bronze.*
workspace.energy_silver.*
```

The ML repository owns forecasting-related:

```text
workspace.energy_gold.*
```

The pipeline must eventually be runnable by Databricks Jobs without requiring a Notebook.

Existing Databricks Notebooks are for exploration, debugging, and learning only.

Production code must not depend on Notebook state.

---

## 14. Secrets

Never hard-code the ENTSO-E API token.

Never commit:

```text
.env
API tokens
Databricks credentials
secret values
```

The production Databricks entry point should retrieve the ENTSO-E token from Databricks Secrets.

Existing secret scope:

```text
energy-demand
```

Do not print secret values to logs.

---

## 15. Testing

Initial tests should focus on logic that can be tested without making real external API calls.

At minimum test:

### Request-window calculation

Given:

```text
2026-09-29 16:37 UTC
```

expect:

```text
request_end   = 2026-09-29 16:00 UTC
request_start = 2026-09-28 16:00 UTC
```

### Timezone handling

Ensure request boundaries are UTC-aware.

### Response validation

Test behavior for:

```text
HTTP 200
non-200 response
empty response
```

Avoid making live ENTSO-E API requests in normal unit tests.

---

## 16. Engineering Principles

Prefer:

- explicit inputs
- small functions
- deterministic time handling
- UTC internally
- clear logging
- retry-safe writes
- reproducibility
- testable business logic
- separation between orchestration and pipeline logic

Avoid:

- hidden global state
- Notebook dependencies
- hard-coded credentials
- silent exception handling
- silent data imputation
- unnecessary abstractions
- premature enterprise complexity

This is a portfolio project, so code should remain understandable to someone reading the repository.

---

## 17. Current Implementation Task

For the first implementation:

1. Create the repository structure defined above.
2. Implement `pipeline/entsoe_bronze.py`.
3. Implement `jobs/run_entsoe_bronze.py`.
4. Add focused unit tests.
5. Add required dependencies to `requirements.txt`.
6. Add appropriate exclusions to `.gitignore`.
7. Write a concise README explaining the current V1 architecture and how the components relate.
8. Do not implement Silver, weather, forecasting, or scheduling yet.
9. Do not modify the existing ML repository.
10. Do not create unnecessary placeholder files for future components.

After implementation, the expected manual validation flow is:

```text
Run production entry point manually in Databricks
              ↓
ENTSO-E request succeeds
              ↓
Raw response written to
workspace.energy_bronze.entsoe_raw
              ↓
Query table manually
              ↓
Confirm metadata + raw XML
              ↓
Only then configure hourly Databricks scheduling
```