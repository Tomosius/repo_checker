#!/usr/bin/env python3
from __future__ import annotations
import os, json, time, threading, argparse, sys, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from typing import List, Dict, Tuple
from src.io_names import read_names_csv, write_results_csv, write_results_json

# ---------- HTTP helper
def http_json(url: str, headers=None, timeout=25) -> Tuple[int, dict]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read().decode("utf-8"))

# ---------- PyPI
def pypi_exists(name: str) -> bool:
    url = f"https://pypi.org/pypi/{urllib.parse.quote(name)}/json"
    try:
        status, _ = http_json(url)
        return status == 200
    except Exception:
        return False

# ---------- Anaconda (conda + conda-forge)
def anaconda_search(name: str) -> Tuple[bool, bool]:
    url = "https://api.anaconda.org/search?" + urllib.parse.urlencode(
        {"q": f"name:{name}", "package_type": "conda"}
    )
    try:
        _, data = http_json(url)
    except Exception:
        return (False, False)

    def norm(s: str) -> str:
        return (s or "").replace("_", "-").lower()

    n = norm(name)
    any_exists = any(norm(pkg.get("name", "")) == n for pkg in data)
    cf_exists = any(
        (pkg.get("owner") == "conda-forge") and (norm(pkg.get("name", "")) == n)
        for pkg in data
    )
    return (cf_exists, any_exists)

# ---------- GitHub (rate-limit aware)
_GH_LOCK = threading.Lock()
_GH_TIMES = deque()  # timestamps of recent calls

def _gh_throttle():
    if os.getenv("GITHUB_TOKEN"):
        return
    with _GH_LOCK:
        now = time.time()
        while _GH_TIMES and now - _GH_TIMES[0] > 60:
            _GH_TIMES.popleft()
        if len(_GH_TIMES) >= 9:
            sleep_for = 60 - (now - _GH_TIMES[0]) + 0.1
            time.sleep(max(0, sleep_for))
        _GH_TIMES.append(time.time())

def github_search(name: str) -> Tuple[int, bool, List[str]]:
    base = "https://api.github.com/search/repositories"
    q = f"{name} in:name"
    params = {"q": q, "per_page": 10}
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "name-availability-checker",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    url = f"{base}?{urllib.parse.urlencode(params)}"
    try:
        _gh_throttle()
        _, data = http_json(url, headers=headers)
        items = data.get("items", []) or []
        total = int(data.get("total_count", 0) or 0)
        exact = any((it.get("name", "").lower() == name.lower()) for it in items)
        urls = [it.get("html_url") for it in items if it.get("html_url")]
        return (total, exact, urls[:10])
    except Exception:
        return (0, False, [])

# ---------- Core
def check_one(name: str) -> Dict:
    pypi = pypi_exists(name)
    cf, any_anaconda = anaconda_search(name)
    gh_count, gh_exact, gh_urls = github_search(name)
    return {
        "name": name,
        "pypi": pypi,
        "conda_forge": cf,
        "anaconda_any": any_anaconda,
        "github_count": gh_count,
        "github_exact": gh_exact,
        "github_top_urls": gh_urls,
    }

# ---------- CLI
def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="Parallel name availability checker (PyPI, Anaconda, GitHub)."
    )
    ap.add_argument("--in", dest="in_csv", required=True, help="Input CSV with names")
    ap.add_argument("--out", dest="out_path", required=True, help="Output file path")
    ap.add_argument(
        "--format", choices=["csv", "json"], default="csv",
        help="Output format (csv or json). Default: csv"
    )
    ap.add_argument(
        "--workers", type=int,
        default=(20 if os.getenv("GITHUB_TOKEN") else 6),
        help="Max worker threads (more if you set GITHUB_TOKEN).",
    )
    ap.add_argument("--print", action="store_true",
                    help="Also print CSV-style rows to stdout as they complete.")
    return ap.parse_args(argv)

def main(argv=None) -> int:
    args = parse_args(argv)
    names = read_names_csv(args.in_csv)
    if not names:
        print("No names found in input CSV.", file=sys.stderr)
        return 2

    results: List[Dict] = []
    print("name,pypi,conda_forge,anaconda_any,github_count,github_exact,github_top_urls")
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(check_one, n): n for n in names}
        for fut in as_completed(futs):
            r = fut.result()
            results.append(r)
            if args.print:
                urls_joined = ";".join(r["github_top_urls"])
                print(f'{r["name"]},{r["pypi"]},{r["conda_forge"]},{r["anaconda_any"]},{r["github_count"]},{r["github_exact"]},"{urls_joined}"')

    # deterministic out
    results.sort(key=lambda d: d["name"].lower())
    if args.format == "csv":
        write_results_csv(args.out_path, results)
    else:
        write_results_json(args.out_path, results)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())