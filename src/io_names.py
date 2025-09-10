# io_names.py
from __future__ import annotations
import csv, json
from typing import Iterable, Dict, List

def read_names_csv(path: str) -> List[str]:
    """
    Accepts either:
      - CSV with a header containing 'name' (case-insensitive), or
      - single-column CSV without header.
    Ignores blank lines and lines starting with '#'.
    De-duplicates while preserving order.
    """
    names: List[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        sample = f.read(1024)
        f.seek(0)
        first_line = sample.splitlines()[0].lower() if sample else ""
        has_header = "name" in first_line

        if has_header:
            f.seek(0)
            for row in csv.DictReader(f):
                val = (row.get("name") or "").strip()
                if val and not val.startswith("#"):
                    names.append(val)
        else:
            f.seek(0)
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                val = (row[0] or "").strip()
                if val and not val.startswith("#") and val.lower() != "name":
                    names.append(val)

    # de-dup while preserving order
    seen = set()
    out: List[str] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out

def write_results_csv(path: str, rows: Iterable[Dict]) -> None:
    fieldnames = [
        "name", "pypi", "conda_forge", "anaconda_any",
        "github_count", "github_exact", "github_top_urls",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            r = dict(r)
            # store URLs as ; separated
            r["github_top_urls"] = ";".join(r.get("github_top_urls", []))
            w.writerow(r)

def write_results_json(path: str, rows: Iterable[Dict]) -> None:
    # keep URLs as list in JSON form
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(rows), f, ensure_ascii=False, indent=2)