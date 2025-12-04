#!/usr/bin/env python3
"""Builds the High-conv and catalyst markdown blocks from JSON files.

The goal is to be tolerant to JSON layout; we only rely on a few field names.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


HC_KEYS: Sequence[str] = ("high_conv", "items", "candidates")
CATALYST_KEYS: Sequence[str] = ("events", "items", "catalysts")


def _load_json(path: Path) -> Any:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _extract_list(data: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, list):
        base = data
    elif isinstance(data, dict):
        base = None
        for k in keys:
            v = data.get(k)
            if isinstance(v, list):
                base = v
                break
        if base is None:
            base = []
    else:
        base = []

    out: List[Dict[str, Any]] = []
    for raw in base:
        if isinstance(raw, dict):
            out.append(raw)
    return out


def build_highconv_block_from_file(path: Path) -> str:
    data = _load_json(path)
    items = _extract_list(data, HC_KEYS)
    if not items:
        return ""
    lines: List[str] = ["### Listán kívüli, 3–12 hónapos high-conv jelöltek", ""]
    for ev in items:
        ticker = (ev.get("ticker") or "").strip().upper()
        thesis = (ev.get("thesis") or ev.get("idea") or "").strip()
        catalyst = (ev.get("catalyst") or ev.get("reason") or "").strip()
        score = ev.get("score")
        parts: List[str] = []
        if ticker:
            parts.append(ticker)
        if thesis:
            parts.append(thesis)
        if catalyst:
            parts.append(catalyst)
        if score not in (None, ""):
            parts.append(f"score={score}")
        if parts:
            lines.append("- " + " – ".join(parts))
    lines.append("")
    return "\n".join(lines)


def build_catalysts_block_from_file(path: Path) -> str:
    data = _load_json(path)
    items = _extract_list(data, CATALYST_KEYS)
    if not items:
        return ""
    lines: List[str] = ["### Közelgő katalizátorok", ""]
    for ev in items:
        ticker = (ev.get("ticker") or "").strip().upper()
        etype = (ev.get("event_type") or ev.get("type") or "").strip()
        date = (ev.get("date_str") or ev.get("date") or "").strip()
        detail = (ev.get("detail") or ev.get("summary") or "").strip()
        parts: List[str] = []
        if ticker:
            parts.append(ticker)
        if etype:
            parts.append(etype)
        if date:
            parts.append(date)
        if detail:
            parts.append(detail)
        if parts:
            lines.append("- " + " – ".join(parts))
    lines.append("")
    return "\n".join(lines)


__all__ = ["build_highconv_block_from_file", "build_catalysts_block_from_file"]
