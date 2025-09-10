import csv
from typing import List

def _read_names_csv(path: str) -> List[str]:
    names: List[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        sample = f.read(1024)
        f.seek(0)
        hdr = (sample.splitlines()[0].lower() if sample else "")
        has_header = "name" in hdr

        if has_header:
            f.seek(0)
            for row in csv.DictReader(f):
                v = (row.get("name") or "").strip()
                if v and not v.startswith("#"):
                    names.append(v)
        else:
            f.seek(0)
            for row in csv.reader(f):
                if not row: continue
                v = (row[0] or "").strip()
                if v and not v.startswith("#") and v.lower() != "name":
                    names.append(v)

    # de-dup preserve order
    seen = set(); out = []
    for n in names:
        if n not in seen:
            seen.add(n); out.append(n)
    return out

_READERS = {
    "csv": _read_names_csv,
    # "txt": _read_names_txt,   # add later
    # "json": _read_names_json, # add later
}

def read_names(path: str, fmt: str = "csv") -> List[str]:
    try:
        return _READERS[fmt](path)
    except KeyError:
        raise ValueError(f"Unsupported input format: {fmt!r}")