from __future__ import annotations

import json
import math
import re
import unicodedata
from datetime import date, datetime
from typing import Any

import pandas as pd


def compact_whitespace(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value.replace("\x0c", " ")).strip()


def ascii_slug(value: str, *, default: str = "field", max_length: int = 48) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_value).strip("_").lower()
    slug = re.sub(r"_+", "_", slug)
    if not slug:
        slug = default
    return slug[:max_length].rstrip("_") or default


def build_excel_sheet_name(prefix: str, serial_number: int, office_name: str) -> str:
    suffix = ascii_slug(office_name, default=f"office_{serial_number}", max_length=22)
    candidate = f"{prefix[:3].upper()}_{serial_number:03d}_{suffix}"
    candidate = candidate[:31]
    return candidate.rstrip("_")


def jsonable_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return str(value)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=jsonable_value)


def maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)
    text = compact_whitespace(str(value))
    if not text:
        return None
    text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def maybe_date_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.date().isoformat()
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()
