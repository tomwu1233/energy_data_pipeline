"""Fetch and persist raw ENTSO-E Actual Total Load responses."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

ENTSOE_ENDPOINT = "https://web-api.tp.entsoe.eu/api"
ENTSOE_AREA = "SE3"
ENTSOE_DOMAIN = "10Y1001A1001A46L"
BRONZE_TABLE = "workspace.energy_bronze.entsoe_raw"
UTC = timezone.utc


@dataclass(frozen=True)
class EntsoeResponse:
    raw_xml: str
    http_status: int
    request_started_at: datetime
    response_received_at: datetime


def calculate_request_window(
    reference_time: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Return the previous 24 hours ending at the last completed UTC hour."""
    if reference_time is None:
        reference_time = datetime.now(UTC)
    if reference_time.tzinfo is None or reference_time.utcoffset() is None:
        raise ValueError("reference_time must be timezone-aware")

    request_end = reference_time.astimezone(UTC).replace(
        minute=0, second=0, microsecond=0
    )
    return request_end - timedelta(hours=24), request_end


def fetch_actual_total_load(
    api_token: str,
    request_start: datetime,
    request_end: datetime,
    http_get: Callable[..., requests.Response] | None = None,
) -> EntsoeResponse:
    """Fetch the ENTSO-E response without interpreting or transforming XML."""
    if http_get is None:
        http_get = requests.get

    request_started_at = datetime.now(UTC)
    response = http_get(
        ENTSOE_ENDPOINT,
        params={
            "securityToken": api_token,
            "documentType": "A65",
            "processType": "A16",
            "outBiddingZone_Domain": ENTSOE_DOMAIN,
            "periodStart": request_start.astimezone(UTC).strftime("%Y%m%d%H%M"),
            "periodEnd": request_end.astimezone(UTC).strftime("%Y%m%d%H%M"),
        },
        timeout=60,
    )
    response_received_at = datetime.now(UTC)
    response.raise_for_status()
    if not response.content:
        raise ValueError("ENTSO-E returned an empty response body")

    return EntsoeResponse(
        raw_xml=response.text,
        http_status=response.status_code,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )


def build_bronze_record(
    ingestion_id: str,
    request_start: datetime,
    request_end: datetime,
    response: EntsoeResponse,
    written_at: datetime | None = None,
) -> dict[str, Any]:
    """Build one immutable-source Bronze row with distinct lifecycle times."""
    return {
        "ingestion_id": ingestion_id,
        "source": "entsoe",
        "area": ENTSOE_AREA,
        "request_start": request_start.astimezone(UTC),
        "request_end": request_end.astimezone(UTC),
        "request_started_at": response.request_started_at,
        "response_received_at": response.response_received_at,
        "written_at": written_at or datetime.now(UTC),
        "http_status": response.http_status,
        "raw_xml": response.raw_xml,
    }


def write_bronze_record(
    spark: Any,
    record: dict[str, Any],
    table_name: str = BRONZE_TABLE,
    delta_table_factory: Any | None = None,
) -> None:
    """Insert once per ingestion ID; a retry of the same ID is a no-op."""
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    if delta_table_factory is None:
        from delta.tables import DeltaTable

        delta_table_factory = DeltaTable

    schema = StructType(
        [
            StructField("ingestion_id", StringType(), nullable=False),
            StructField("source", StringType(), nullable=False),
            StructField("area", StringType(), nullable=False),
            StructField("request_start", TimestampType(), nullable=False),
            StructField("request_end", TimestampType(), nullable=False),
            StructField("request_started_at", TimestampType(), nullable=False),
            StructField("response_received_at", TimestampType(), nullable=False),
            StructField("written_at", TimestampType(), nullable=False),
            StructField("http_status", IntegerType(), nullable=False),
            StructField("raw_xml", StringType(), nullable=False),
        ]
    )
    source = spark.createDataFrame([record], schema=schema)
    target = delta_table_factory.forName(spark, table_name)
    (
        target.alias("target")
        .merge(
            source.alias("source"),
            "target.ingestion_id = source.ingestion_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_entsoe_bronze(
    spark: Any,
    api_token: str,
    ingestion_id: str,
    table_name: str = BRONZE_TABLE,
    reference_time: datetime | None = None,
    http_get: Callable[..., requests.Response] | None = None,
    delta_table_factory: Any | None = None,
) -> dict[str, Any]:
    """Run one raw ENTSO-E ingestion and persist its Bronze record."""
    request_start, request_end = calculate_request_window(reference_time)
    response = fetch_actual_total_load(
        api_token,
        request_start,
        request_end,
        http_get=http_get,
    )
    record = build_bronze_record(
        ingestion_id,
        request_start,
        request_end,
        response,
    )
    write_bronze_record(
        spark,
        record,
        table_name=table_name,
        delta_table_factory=delta_table_factory,
    )
    return record