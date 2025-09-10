#!/usr/bin/env python3
from __future__ import annotations
import os, json, time, threading, argparse, sys, urllib.request, urllib.parse, pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from typing import List, Dict, Tuple, Any
from input_output import read_names, write_results

# ---------- tiny JSONPath-lite (just enough for our config)
def _json_read(doc: Any, path: str) -> List[Any]:
    if path == "$":
        return [doc]
    parts = path.strip().lstrip("$").lstrip(".").split(".")
    cur = [doc]
    for p in parts:
        nxt = []
        if p == "[*]":
            for node in cur:
                if isinstance(node, list):
                    nxt.extend(node)
        elif p.endswith("[*]"):
            key = p[:-3]
            for node in cur:
                if isinstance(node, dict):
                    seq = node.get(key, [])
                    if isinstance(seq, list):
                        nxt.extend(seq)
        else:
            for node in cur:
                if isinstance(node, dict) and p in node:
                    nxt.append(node[p])
        cur = nxt
    return cur

def _norm(s: Any, lower=False, unders_to_dashes=False) -> str:
    s = "" if s is None else str(s)
    if unders_to_dashes:
        s = s.replace("_", "-")
    return s.lower() if lower else s

# ---------- HTTP helper
from urllib.error import HTTPError, URLError

def http_json(url: str, headers=None, timeout=25) -> Tuple[int, dict | list | None]:
    headers = {k: v for k, v in (headers or {}).items() if v}
    req = urllib.request.Request(url, headers=headers)

    body = ""
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status = r.status
            body = r.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        # e.g. PyPI returns 404 for missing packages — that’s fine
        status = e.code
        try:
            body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        except Exception:
            body = ""
    except URLError:
        # network/DNS/etc.
        return 0, None
    except Exception:
        return 0, None

    try:
        data = json.loads(body) if body else None
    except Exception:
        data = None
    return status, data

# ---------- generic throttle (used by engines that opt-in)
_TH_LOCK = threading.Lock()
_TH_TIMES = deque()
def throttle(max_per_minute: int):
    with _TH_LOCK:
        now = time.time()
        while _TH_TIMES and now - _TH_TIMES[0] > 60:
            _TH_TIMES.popleft()
        if len(_TH_TIMES) >= max_per_minute:
            time.sleep(max(0.0, 60 - (now - _TH_TIMES[0]) + 0.05))
        _TH_TIMES.append(time.time())

def _expand_env(v: str) -> str:
    return os.path.expandvars(v) if isinstance(v, str) else v

# ---------- engine runner driven by config
def run_engine(engine: Dict, query: str) -> Dict:
    url = engine["url"].format(q=urllib.parse.quote(query))
    headers = {k: _expand_env(v) for k, v in (engine.get("headers") or {}).items()}
    th = engine.get("throttle") or {}
    if th and not os.getenv(th.get("env_bypass", "")):
        throttle(int(th.get("max_per_minute", 9)))

    status, data = http_json(url, headers=headers)

    # default outputs
    out = {"exists": False, "count": 0, "exact": False, "urls": [], "status": status}

    ex = engine.get("exists")
    if ex and ex.get("kind") == "status_is":
        out["exists"] = (status == int(ex.get("code", 200)))
    elif ex and ex.get("kind") == "json_any_eq" and isinstance(data, (list, dict)):
        vals = _json_read(data, ex["path"])
        qn = _norm(query,
                   lower=ex.get("normalize", {}).get("to_lower", False),
                   unders_to_dashes=ex.get("normalize", {}).get("replace_underscores_with_dashes", False))
        out["exists"] = any(
            _norm(v,
                  lower=ex.get("normalize", {}).get("to_lower", False),
                  unders_to_dashes=ex.get("normalize", {}).get("replace_underscores_with_dashes", False)) == qn
            for v in vals
        )
    elif ex and ex.get("kind") == "json_any_match" and isinstance(data, (list, dict)):
        crits = ex["where"]
        cols = [_json_read(data, c["path"]) for c in crits]
        maxlen = max((len(col) for col in cols), default=0)
        def val(col, i): return col[i] if i < len(col) else None
        for i in range(maxlen):
            ok = True
            for c, col in zip(crits, cols):
                v = val(col, i)
                tgt = query if c.get("equals") == "{q}" else c.get("equals")
                v = _norm(v,
                          lower=c.get("normalize", {}).get("to_lower", False),
                          unders_to_dashes=c.get("normalize", {}).get("replace_underscores_with_dashes", False))
                tgt = _norm(tgt or "",
                            lower=c.get("normalize", {}).get("to_lower", False),
                            unders_to_dashes=c.get("normalize", {}).get("replace_underscores_with_dashes", False))
                if v != tgt:
                    ok = False
                    break
            if ok:
                out["exists"] = True
                break

    res = engine.get("result") or {}
    if res.get("count_path") and isinstance(data, (list, dict)):
        vals = _json_read(data, res["count_path"])
        if vals:
            try: out["count"] = int(vals[0])
            except Exception: pass
    if res.get("exact_any_path") and isinstance(data, (list, dict)):
        names = _json_read(data, res["exact_any_path"])
        out["exact"] = any(str(n).lower() == query.lower() for n in names)
    if res.get("urls_path") and isinstance(data, (list, dict)):
        out["urls"] = [str(u) for u in _json_read(data, res["urls_path"]) if u][:10]

    return out

def load_engines(path: str) -> List[Dict]:
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml  # pip install pyyaml
        except ImportError:
            print("Please `pip install pyyaml` to use YAML configs.", file=sys.stderr)
            raise
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)["engines"]
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["engines"]

# ---------- Core
def check_one(name: str, engines: List[Dict]) -> Dict:
    out = {
        "name": name,
        "pypi": False,
        "conda_forge": False,
        "anaconda_any": False,
        "github_count": 0,
        "github_exact": False,
        "github_top_urls": [],
    }
    for eng in engines:
        r = run_engine(eng, name)
        eid = eng["id"]
        if eid == "pypi":
            out["pypi"] = r["exists"]
        elif eid == "conda_forge":
            out["conda_forge"] = r["exists"]
        elif eid == "anaconda_any":
            out["anaconda_any"] = r["exists"]
        elif eid == "github":
            out["github_count"] = r["count"]
            out["github_exact"] = r["exact"]
            out["github_top_urls"] = r["urls"]
    return out

# ---------- CLI
def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="Parallel name availability checker (config-driven)."
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
    ap.add_argument("--engines", default=None,
                    help="Path to engines.yaml/json (default: engines.yaml next to main.py)")
    return ap.parse_args(argv)

def main(argv=None) -> int:
    args = parse_args(argv)
    cfg_path = args.engines or str(pathlib.Path(__file__).with_name("engines.yaml"))
    engines = load_engines(cfg_path)

    names = read_names(args.in_csv, fmt="csv")
    if not names:
        print("No names found in input CSV.", file=sys.stderr)
        return 2

    print("name,pypi,conda_forge,anaconda_any,github_count,github_exact,github_top_urls")
    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(check_one, n, engines): n for n in names}
        for fut in as_completed(futs):
            r = fut.result()
            results.append(r)
            if args.print:
                urls_joined = ";".join(r["github_top_urls"])
                print(f'{r["name"]},{r["pypi"]},{r["conda_forge"]},{r["anaconda_any"]},'
                      f'{r["github_count"]},{r["github_exact"]},"{urls_joined}"')

    results.sort(key=lambda d: d["name"].lower())
    write_results(args.out_path, results, fmt=args.format)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())