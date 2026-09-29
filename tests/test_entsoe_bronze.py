from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import requests

from pipeline.entsoe_bronze import (
    BRONZE_TABLE,
    ENTSOE_DOMAIN,
    EntsoeResponse,
    build_bronze_record,
    calculate_request_window,
    fetch_actual_total_load,
    write_bronze_record,
)

UTC = timezone.utc


def test_request_window_ends_at_last_completed_utc_hour() -> None:
    start, end = calculate_request_window(datetime(2026, 9, 29, 16, 37, tzinfo=UTC))

    assert start == datetime(2026, 9, 28, 16, 0, tzinfo=UTC)
    assert end == datetime(2026, 9, 29, 16, 0, tzinfo=UTC)


def test_request_window_normalizes_offset_to_utc() -> None:
    local_time = datetime(2026, 9, 29, 18, 37, tzinfo=timezone(timedelta(hours=2)))

    start, end = calculate_request_window(local_time)

    assert start == datetime(2026, 9, 28, 16, 0, tzinfo=UTC)
    assert end == datetime(2026, 9, 29, 16, 0, tzinfo=UTC)


def test_request_window_rejects_naive_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        calculate_request_window(datetime(2026, 9, 29, 16, 37))


def test_fetch_preserves_xml_and_sends_expected_query() -> None:
    raw_xml = '<?xml version="1.0"?><Publication_MarketDocument />'
    captured: dict[str, object] = {}

    class Response:
        content = raw_xml.encode("utf-8")
        text = raw_xml
        status_code = 200

        def raise_for_status(self) -> None:
            return None

    def fake_get(url: str, **kwargs: object) -> Response:
        captured["url"] = url
        captured.update(kwargs)
        return Response()

    result = fetch_actual_total_load(
        "test-token",
        datetime(2026, 9, 28, 16, 0, tzinfo=UTC),
        datetime(2026, 9, 29, 16, 0, tzinfo=UTC),
        http_get=fake_get,
    )

    assert result.raw_xml == raw_xml
    assert result.http_status == 200
    assert captured["params"] == {
        "securityToken": "test-token",
        "documentType": "A65",
        "processType": "A16",
        "outBiddingZone_Domain": ENTSOE_DOMAIN,
        "periodStart": "202609281600",
        "periodEnd": "202609291600",
    }


def test_fetch_surfaces_http_errors() -> None:
    class Response:
        content = b"error"
        status_code = 500
        text = "error"

        def raise_for_status(self) -> None:
            raise requests.HTTPError("server error")

    with pytest.raises(requests.HTTPError, match="server error"):
        fetch_actual_total_load(
            "test-token",
            datetime(2026, 9, 28, 16, 0, tzinfo=UTC),
            datetime(2026, 9, 29, 16, 0, tzinfo=UTC),
            http_get=lambda *args, **kwargs: Response(),
        )


def test_bronze_record_keeps_lifecycle_timestamps_separate() -> None:
    request_start = datetime(2026, 9, 28, 16, 0, tzinfo=UTC)
    request_end = datetime(2026, 9, 29, 16, 0, tzinfo=UTC)
    request_started = datetime(2026, 9, 29, 16, 1, tzinfo=UTC)
    response_received = datetime(2026, 9, 29, 16, 2, tzinfo=UTC)
    written = datetime(2026, 9, 29, 16, 3, tzinfo=UTC)
    response = EntsoeResponse("<xml />", 200, request_started, response_received)

    record = build_bronze_record(
        "ingestion-1", request_start, request_end, response, written_at=written
    )

    assert record["request_started_at"] == request_started
    assert record["response_received_at"] == response_received
    assert record["written_at"] == written
    assert record["raw_xml"] == "<xml />"


def test_bronze_write_is_insert_only_for_an_ingestion_id() -> None:
    calls: dict[str, object] = {}

    class DataFrame:
        def alias(self, name: str) -> DataFrame:
            return self

    class Spark:
        def createDataFrame(self, rows: list[dict[str, object]], schema: object) -> DataFrame:
            calls["rows"] = rows
            return DataFrame()

    class Merge:
        def whenNotMatchedInsertAll(self) -> Merge:
            calls["insert_only"] = True
            return self

        def execute(self) -> None:
            calls["executed"] = True

    class DeltaTarget:
        def alias(self, name: str) -> DeltaTarget:
            return self

        def merge(self, source: DataFrame, condition: str) -> Merge:
            calls["condition"] = condition
            return Merge()

    class DeltaFactory:
        @staticmethod
        def forName(spark: Spark, table_name: str) -> DeltaTarget:
            calls["table_name"] = table_name
            return DeltaTarget()

    write_bronze_record(
        Spark(),
        {"ingestion_id": "ingestion-1"},
        delta_table_factory=DeltaFactory,
    )

    assert calls["table_name"] == BRONZE_TABLE
    assert calls["condition"] == "target.ingestion_id = source.ingestion_id"
    assert calls["insert_only"] is True
    assert calls["executed"] is True