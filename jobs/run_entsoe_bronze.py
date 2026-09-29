"""Databricks Jobs entry point for ENTSO-E Bronze ingestion."""

from __future__ import annotations

import os
from uuid import NAMESPACE_URL, uuid4, uuid5

from databricks.sdk.runtime import dbutils, spark

from pipeline.entsoe_bronze import BRONZE_TABLE, run_entsoe_bronze


def _get_ingestion_id() -> str:
    override = os.environ.get("INGESTION_ID")
    if override:
        return override

    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    job_run_id = context.tags().get("jobRunId")
    if job_run_id.isDefined():
        return str(uuid5(NAMESPACE_URL, f"entsoe-bronze:{job_run_id.get()}"))
    return str(uuid4())


def main() -> None:
    secret_scope = os.environ["ENTSOE_SECRET_SCOPE"]
    secret_key = os.environ["ENTSOE_SECRET_KEY"]
    api_token = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    table_name = os.environ.get("BRONZE_TABLE", BRONZE_TABLE)

    run_entsoe_bronze(
        spark=spark,
        api_token=api_token,
        ingestion_id=_get_ingestion_id(),
        table_name=table_name,
    )


if __name__ == "__main__":
    main()