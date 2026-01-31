"""Shared Parquet / PyArrow schemas for raw-layer data."""

from __future__ import annotations

import pyarrow as pa

PORT_OPS_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("port_name", pa.string()),
        pa.field("teu_total", pa.float64()),
        pa.field("vessel_calls", pa.int32()),
        pa.field("avg_vessel_wait_hours", pa.float64()),
        pa.field("terminal_dwell_days", pa.float64()),
        pa.field("rail_dwell_days", pa.float64()),
        pa.field("truck_turn_time_min", pa.float64()),
        pa.field("source_record_count", pa.int32()),
        pa.field("source_min_ts", pa.timestamp("us")),
        pa.field("source_max_ts", pa.timestamp("us")),
    ]
)

COMTRADE_SCHEMA = pa.schema(
    [
        pa.field("period", pa.int32()),  # YYYYMM
        pa.field("reporter_iso3", pa.string()),
        pa.field("partner_iso3", pa.string()),
        pa.field("hs_code", pa.string()),
        pa.field("trade_flow", pa.string()),  # import / export
        pa.field("trade_value_usd", pa.float64()),
        pa.field("net_weight_kg", pa.float64()),
        pa.field("quantity", pa.float64()),
        pa.field("quantity_unit", pa.string()),
    ]
)

GDELT_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("country_iso3", pa.string()),
        pa.field("event_code", pa.string()),
        pa.field("event_count", pa.int32()),
        pa.field("avg_tone", pa.float64()),
    ]
)

NOAA_SCHEMA = pa.schema(
    [
        pa.field("date", pa.date32()),
        pa.field("station_id", pa.string()),
        pa.field("station_name", pa.string()),
        pa.field("temp_avg_c", pa.float64()),
        pa.field("precipitation_mm", pa.float64()),
        pa.field("wind_avg_mps", pa.float64()),
        pa.field("storm_flag", pa.bool_()),
    ]
)

MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("run_date", pa.date32()),
        pa.field("source_name", pa.string()),
        pa.field("status", pa.string()),
        pa.field("last_partition_loaded", pa.string()),
        pa.field("rows_loaded", pa.int64()),
        pa.field("error_count", pa.int64()),
        pa.field("latency_hours", pa.float64()),
    ]
)
