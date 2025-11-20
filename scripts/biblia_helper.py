
"""
biblia_helper.py

Placeholder helper for the #1 / #2 / #3 jelentések "biblia" szabálykönyvéhez.

Jelenleg csak üres, de a struktúra adott:
- get_report1_checklist()
- get_report2_checklist()
- get_report3_checklist()

Ha később elveszne a topik vagy a beszélgetés,
ez a fájl fogja tartalmazni a kanonikus ellenőrzőlistákat,
hogy mit KELL tartalmaznia minden jelentésnek.
"""


from __future__ import annotations
from typing import List


def get_report1_checklist() -> List[str]:
    """
    #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST

    Jelenleg üres. Később ide kerül:
    - minden kötelező blokk felsorolása,
    - a forrás-prioritások,
    - az időablakok,
    - a mozgás-küszöbök,
    - és a speciális szabályok (high-conviction, eladott pozíciók stb.).
    """
    return []


def get_report2_checklist() -> List[str]:
    """
    #2 – Tegnapi nyitástól zárásig (Open→Close) jelentés checklist.

    Jelenleg üres – később lesz feltöltve, miután a #2-es jelentés 100%-ban
    bíblia-kompatibilisre finomhangolva lett.
    """
    return []


def get_report3_checklist() -> List[str]:
    """
    #3 – Ma nyitástól mostanáig (Open→Most) jelentés checklist.

    Ugyanaz az elv, mint #2-nél: először a scriptet hozzuk bíblia-szintre,
    utána kerülnek ide a kanonikus pontok.
    """
    return []
