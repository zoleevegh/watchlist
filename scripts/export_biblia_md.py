# scripts/export_biblia_md.py
# -*- coding: utf-8 -*-

from pathlib import Path
from biblia_helper import BIBLIA_SPEC_MD


def main() -> None:
    docs_dir = Path("docs")
    docs_dir.mkdir(exist_ok=True)
    out_path = docs_dir / "biblia_leiras.md"
    out_path.write_text(BIBLIA_SPEC_MD, encoding="utf-8")
    print(f"biblia_leiras.md frissítve: {out_path}")


if __name__ == "__main__":
    main()
