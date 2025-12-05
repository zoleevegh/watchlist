# blocks_intraday_3.py
# Version: v1.1.0
"""Intraday (Open→Most) block builder for report #3.

This module is intentionally defensive:
- It accepts `latest_json` either as:
  * already-parsed Python dict / list, OR
  * a JSON string.
- It tolerates missing / partial fields and tries to produce a sensible
  markdown output instead of raising.
- It does **not** maga számolja az Open→Most %-okat, csak a már
  előkészített adatot formázza.

Várt séma (ajánlott, nem kötelező – a kód ennél lazábban kezel):
latest_json = {
    "items": [
        {
            "ticker": "NVDA",
            "percent": 4.91,            # Open→Most %
            "is_position": true,        # darabszámos ticker
            "is_watchlist": false,
            "has_news": true,           # van-e releváns hír
            "news_reason": "Reuters: ..."  # 1 mondatos indok
        },
        ...
    ]
}

A logika:
- Darabszámos tickerek: mindig bekerülnek.
- Watchlist: csak ha ≥±3.00% VAGY van hír/indok.
- A %-okat két tizedre kerekítjük.
"""

import json
from typing import Any, Dict, Iterable, List, Tuple


def _ensure_items(latest_json: Any) -> List[Dict[str, Any]]:
    """Normalize `latest_json` to a list of dict items.

    Elfogadott input:
    - JSON string
    - dict, amelyben 'items' vagy 'data' kulcs alatt van a lista
    - közvetlen list of dict
    Minden más esetben üres listát ad vissza.
    """
    if latest_json is None:
        return []

    # Ha string, próbáljuk JSON-ként értelmezni.
    if isinstance(latest_json, str):
        latest_json = latest_json.strip()
        if not latest_json:
            return []
        try:
            latest_json = json.loads(latest_json)
        except Exception:
            return []

    # Dict eset: keressünk benne 'items' vagy 'data' kulcsot.
    if isinstance(latest_json, dict):
        items = (
            latest_json.get("items")
            or latest_json.get("data")
            or latest_json.get("tickers")
        )
        if items is None:
            return []
    else:
        items = latest_json

    # Ha lista, szűrjük a dict típusú elemekre.
    if isinstance(items, list):
        norm: List[Dict[str, Any]] = []
        for raw in items:
            if isinstance(raw, dict):
                norm.append(raw)
        return norm

    return []


def _to_float(val: Any) -> float:
    """Próbálja lebegőre konvertálni a bemenetet; hibánál None-t ad.

    A 3%-os küszöb miatt fontos a számítás pontossága,
    de ha a mező nincs jelen, inkább None-t adunk vissza, mint hibát dobunk.
    """
    if val is None:
        return None  # type: ignore[return-value]
    if isinstance(val, (int, float)):
        return float(val)
    try:
        sval = str(val).strip().replace("%", "")
        if not sval:
            return None  # type: ignore[return-value]
        return float(sval)
    except Exception:
        return None  # type: ignore[return-value]


def _format_percent(p: float) -> str:
    """Formázza a százalékot két tizedre, előjellel.

    Példák:
    -  3.1  -> '+3.10%'
    - -2.987 -> '-2.99%'
    """
    sign = "+" if p >= 0 else ""
    return f"{sign}{p:.2f}%"


def _split_positions_watchlist(items: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Kettéválasztja az itemeket pozíciókra és watchlistre.

    Heurisztikák:
    - Ha 'is_position' True → pozíció.
    - Ha 'is_watchlist' True és nem pozíció → watchlist.
    - Ha egyik sincs, de 'shares' > 0 → pozíció.
    - Minden egyéb ticker watchlist-be kerül.
    """
    positions: List[Dict[str, Any]] = []
    watchlist: List[Dict[str, Any]] = []

    for item in items:
        is_position = bool(item.get("is_position") or item.get("position"))
        is_watch = bool(item.get("is_watchlist") or item.get("watchlist"))

        shares = item.get("shares") or item.get("qty") or item.get("quantity")
        try:
            shares_num = float(shares) if shares is not None else 0.0
        except Exception:
            shares_num = 0.0

        if is_position or shares_num > 0:
            positions.append(item)
        elif is_watch:
            watchlist.append(item)
        else:
            # Ha semmi infó, tegyük watchlistbe, hogy ne vesszen el teljesen.
            watchlist.append(item)

    return positions, watchlist


def _format_line(item: Dict[str, Any]) -> str:
    """Egyetlen ticker sor formázása markdown bulletként.

    Formátum:
    - 'TICKER +3.45% – ok: ...'
    - ha nincs % adat: 'TICKER – nincs elérhető intraday adat'
    """
    ticker = str(item.get("ticker") or item.get("symbol") or "").strip().upper()
    if not ticker:
        ticker = "ISMERETLEN"

    p = _to_float(item.get("percent") or item.get("pct_change") or item.get("change_pct"))
    has_news = bool(item.get("has_news") or item.get("news_flag"))
    reason = str(item.get("news_reason") or item.get("reason") or "").strip()

    if p is None:
        core = f"{ticker} – nincs elérhető intraday adat"
    else:
        core = f"{ticker} {_format_percent(p)}"
    if has_news and reason:
        core += f" – ok: {reason}"

    return f"- {core}"


def build_intraday_blocks(latest_json: Any) -> Tuple[str, str]:
    """Felépíti a #3 riport intraday (Open→Most) markdown blokkjait.

    Visszatérés:
        (positions_block_md, watchlist_block_md)
    Mindkettő teljes markdown szöveg, címsorral együtt.
    """
    items = _ensure_items(latest_json)

    header_pos = "### Darabszámos tickerek – Open→Most"
    header_wl = "### Watchlist – releváns ticker-mozgások"

    if not items:
        # Ha semmilyen adat nincs, adjunk vissza üres, de szöveges blokkokat.
        positions_block = header_pos + "\nNincs elérhető intraday adat a darabszámos tickerekre.\n"
        watchlist_block = header_wl + "\nNincs releváns watchlist mozgás.\n"
        return positions_block, watchlist_block

    positions, watchlist = _split_positions_watchlist(items)

    # Sorok generálása pozíciókra: minden pozíció bekerül.
    pos_lines: List[str] = [header_pos]
    if positions:
        # Rendezés abszolút % szerint, csökkenő.
        def pos_key(it: Dict[str, Any]) -> float:
            p = _to_float(it.get("percent") or it.get("pct_change") or it.get("change_pct"))
            return abs(p) if p is not None else 0.0

        for item in sorted(positions, key=pos_key, reverse=True):
            pos_lines.append(_format_line(item))
    else:
        pos_lines.append("Nincs darabszámos ticker az intraday listában.")

    # Watchlist: csak ha ≥3% vagy van hír/indok.
    wl_lines: List[str] = [header_wl]
    filtered_watchlist: List[Dict[str, Any]] = []
    for item in watchlist:
        p = _to_float(item.get("percent") or item.get("pct_change") or item.get("change_pct"))
        has_news = bool(item.get("has_news") or item.get("news_flag"))
        reason = str(item.get("news_reason") or item.get("reason") or "").strip()

        include = False
        if p is not None and abs(p) >= 3.0:
            include = True
        if has_news or reason:
            include = True

        if include:
            filtered_watchlist.append(item)

    if filtered_watchlist:
        def wl_key(it: Dict[str, Any]) -> float:
            p = _to_float(it.get("percent") or it.get("pct_change") or it.get("change_pct"))
            return abs(p) if p is not None else 0.0

        for item in sorted(filtered_watchlist, key=wl_key, reverse=True):
            wl_lines.append(_format_line(item))
    else:
        wl_lines.append("Nincs olyan watchlist ticker, ami ma hírrel vagy ≥±3.00%-os mozgással kiemelkedett volna.")

    positions_block = "\n".join(pos_lines) + "\n"
    watchlist_block = "\n".join(wl_lines) + "\n"
    return positions_block, watchlist_block
