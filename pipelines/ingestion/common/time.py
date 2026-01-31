"""Date/time helpers for partitioning and formatting."""

from __future__ import annotations

from datetime import date, datetime, timedelta


def parse_date(date_str: str) -> date:
    """Parse YYYY-MM-DD string into a date object."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def date_key(d: date) -> int:
    """Convert a date to an integer YYYYMMDD key."""
    return int(d.strftime("%Y%m%d"))


def month_key(d: date) -> int:
    """Convert a date to an integer YYYYMM key."""
    return int(d.strftime("%Y%m"))


def partition_path(source: str, d: date) -> str:
    """Build a Hive-style partition path: raw/<source>/dt=YYYY-MM-DD/"""
    return f"raw/{source}/dt={d.isoformat()}/"


def month_partition_path(source: str, d: date) -> str:
    """Build a monthly partition path: raw/<source>/month=YYYY-MM/"""
    return f"raw/{source}/month={d.strftime('%Y-%m')}/"


def date_range(start: date, end: date) -> list[date]:
    """Return a list of dates from start to end (inclusive)."""
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days
