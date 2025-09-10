import csv, json
from typing import Iterable, Dict

def _write_results_csv(path: str, rows: Iterable[Dict]) -> None:
    fields = ["name","pypi","conda_forge","anaconda_any",
              "github_count","github_exact","github_top_urls"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            r = dict(r)
            r["github_top_urls"] = ";".join(r.get("github_top_urls", []))
            w.writerow(r)

def _write_results_json(path: str, rows: Iterable[Dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(rows), f, ensure_ascii=False, indent=2)

_WRITERS = {
    "csv": _write_results_csv,
    "json": _write_results_json,
    # "html": _write_results_html, # add later
}

def write_results(path: str, rows: Iterable[Dict], fmt: str = "csv") -> None:
    try:
        _WRITERS[fmt](path, rows)
    except KeyError:
        raise ValueError(f"Unsupported output format: {fmt!r}")