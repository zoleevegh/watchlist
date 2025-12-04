#!/usr/bin/env python3
# postprocess_report_1_v1_0_1.py
# Version: 1.0.1 - Biblia szerinti könyvtárszerkezetre frissítve

import json
from pathlib import Path
from typing import Any, List, Sequence


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _extract_list_like(data: Any, keys: Sequence[str]) -> List[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in keys:
            v = data.get(key)
            if isinstance(v, list):
                return v
    return []


def build_macro_block(macro_data: Any, max_items: int = 5) -> str:
    if not macro_data:
        return ""
    items = _extract_list_like(macro_data, ("macro_news", "items", "news", "headlines"))
    if not items:
        return ""

    out: List[str] = ["### Politika / FED / Makró", ""]
    for raw in items[:max_items]:
        if isinstance(raw, str):
            out.append(f"- {raw.strip()}")
        elif isinstance(raw, dict):
            h = (raw.get("headline") or raw.get("title") or raw.get("summary") or "").strip()
            s = (raw.get("snippet") or raw.get("description") or raw.get("note") or "").strip()
            src = (raw.get("source") or "").strip()
            ts = (raw.get("time") or raw.get("timestamp") or "").strip()
            parts = [p for p in (h, s) if p]
            meta = " | ".join([p for p in (src, ts) if p])
            if meta:
                parts.append(meta)
            if parts:
                out.append(f"- {' — '.join(parts)}")
    out.append("")
    return "\n".join(out)


def build_highconv_block(data: Any) -> str:
    if not data:
        return ""
    items = _extract_list_like(data, ("high_conv", "items", "candidates"))
    if not items:
        return ""
    out: List[str] = ["### Listán kívüli, 3–12 hónapos high-conv jelöltek", ""]
    for raw in items:
        if isinstance(raw, str):
            out.append(f"- {raw.strip()}")
        elif isinstance(raw, dict):
            t = (raw.get("ticker") or "").strip()
            th = (raw.get("thesis") or raw.get("summary") or "").strip()
            c = (raw.get("catalyst") or "").strip()
            parts = [p for p in (t, th, c) if p]
            if parts:
                out.append(f"- {' – '.join(parts)}")
    out.append("")
    return "\n".join(out)


def build_analyst_block(data: Any) -> str:
    if not data:
        return ""
    items = _extract_list_like(data, ("events", "items", "analyst_actions"))
    if not items:
        return ""
    out: List[str] = ["### Bejelentések & fel/lemínősítések", ""]
    for raw in items:
        if isinstance(raw, str):
            out.append(f"- {raw.strip()}")
        elif isinstance(raw, dict):
            t = (raw.get("ticker") or "").strip()
            a = (raw.get("action") or "").strip()
            f = (raw.get("firm") or "").strip()
            an = (raw.get("analyst") or "").strip()
            d = (raw.get("detail") or raw.get("summary") or "").strip()
            who = ", ".join([p for p in (f, an) if p])
            parts = [p for p in (t, a, who, d) if p]
            if parts:
                out.append(f"- {' – '.join(parts)}")
    out.append("")
    return "\n".join(out)


def build_catalysts_block(data: Any) -> str:
    if not data:
        return ""
    items = _extract_list_like(data, ("events", "items", "catalysts"))
    if not items:
        return ""
    out: List[str] = ["### Közelgő katalizátorok", ""]
    for raw in items:
        if isinstance(raw, str):
            out.append(f"- {raw.strip()}")
        elif isinstance(raw, dict):
            t = (raw.get("ticker") or "").strip()
            et = (raw.get("event_type") or raw.get("type") or "").strip()
            dt = (raw.get("date_str") or raw.get("date") or "").strip()
            d = (raw.get("detail") or raw.get("summary") or "").strip()
            parts = [p for p in (t, et, dt, d) if p]
            if parts:
                out.append(f"- {' – '.join(parts)}")
    out.append("")
    return "\n".join(out)


def insert_macro_block(md: str, macro_block: str) -> str:
    if not macro_block:
        return md
    lines = md.splitlines()
    idx = next((i for i, l in enumerate(lines) if l.strip().startswith("Lefedettség:")), None)
    if idx is None:
        return md + "\n\n" + macro_block
    insert_pos = idx + 1
    while insert_pos < len(lines) and lines[insert_pos].strip():
        insert_pos += 1
    new_lines = lines[: insert_pos + 1] + [macro_block] + lines[insert_pos + 1 :]
    return "\n".join(new_lines)


def strip_job_summary(md: str) -> str:
    return "\n".join(
        [l for l in md.splitlines() if not l.strip().startswith("Job summary generated")]
    ).rstrip()


def append_blocks(md: str, *blocks: str) -> str:
    out: List[str] = [md.rstrip()]
    for b in blocks:
        if b:
            out.append("")
            out.append(b.rstrip())
    return "\n\n".join(out) + "\n"


def main() -> None:
    base = Path("reports/1")

    md_path = base / "summary_report_1.md"
    macro_json = load_json(base / "macro_news_1.json")
    analyst_json = load_json(base / "analyst_1.json")
    catalysts_json = load_json(base / "catalysts_1.json")
    highconv_json = load_json(base / "high_conv_1.json")

    md = read_text(md_path)

    macro_block = build_macro_block(macro_json)
    md = insert_macro_block(md, macro_block)

    md = strip_job_summary(md)

    analyst_block = build_analyst_block(analyst_json)
    catalysts_block = build_catalysts_block(catalysts_json)
    highconv_block = build_highconv_block(highconv_json)

    md = append_blocks(md, analyst_block, catalysts_block, highconv_block)

    write_text(md_path, md)


if __name__ == "__main__":
    main()
