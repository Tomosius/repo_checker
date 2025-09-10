# check_names_parallel.py
import os, json, time, threading
import urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

NAMES = [
    "morphkit","morphflow","morphforge","morphworks","morfit","morphlane",
    "morphcore","morphcraft","streamfit","batchfit","strideml","chunklab",
    "spillway","featherflow","columml","columna","quiverml","fletchml",
    "forgeflo","shapefit","transfit","vectorforge","deltakit",
]

# ---- HTTP helper
def http_json(url, headers=None, timeout=25):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read().decode("utf-8"))

# ---- PyPI
def pypi_exists(name: str) -> bool:
    url = f"https://pypi.org/pypi/{urllib.parse.quote(name)}/json"
    try:
        status, _ = http_json(url)
        return status == 200
    except Exception:
        return False

# ---- Anaconda (conda + conda-forge)
def anaconda_search(name: str):
    url = "https://api.anaconda.org/search?" + urllib.parse.urlencode(
        {"q": f"name:{name}", "package_type": "conda"}
    )
    try:
        _, data = http_json(url)
    except Exception:
        return (False, False)

    norm = lambda s: s.replace("_", "-").lower()
    n = norm(name)
    any_exists = any(norm(pkg.get("name", "")) == n for pkg in data)
    cf_exists = any(
        (pkg.get("owner") == "conda-forge") and (norm(pkg.get("name", "")) == n)
        for pkg in data
    )
    return (cf_exists, any_exists)

# ---- GitHub with shared rate-limit (unauthenticated ~10/min)
_GH_LOCK = threading.Lock()
_GH_TIMES = deque()  # timestamps of recent calls

def _gh_throttle():
    if os.getenv("GITHUB_TOKEN"):
        return  # token lifts us to much higher limits
    with _GH_LOCK:
        now = time.time()
        # Keep only last 60s window
        while _GH_TIMES and now - _GH_TIMES[0] > 60:
            _GH_TIMES.popleft()
        # Allow up to 9 calls per minute to be safe
        if len(_GH_TIMES) >= 9:
            sleep_for = 60 - (now - _GH_TIMES[0]) + 0.1
            time.sleep(max(0, sleep_for))
        _GH_TIMES.append(time.time())

def github_search(name: str):
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
        return (total, exact, urls)
    except Exception:
        return (0, False, [])

def check_one(name: str):
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
        "github_top_urls": gh_urls[:10],
    }

def main():
    # Tune workers: with token you can go higher safely.
    max_workers = 20 if os.getenv("GITHUB_TOKEN") else 6
    print("name,pypi,conda_forge,anaconda_any,github_count,github_exact,github_top_urls")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(check_one, n): n for n in NAMES}
        for fut in as_completed(futures):
            r = fut.result()
            urls_joined = ";".join(r["github_top_urls"])
            print(f'{r["name"]},{r["pypi"]},{r["conda_forge"]},{r["anaconda_any"]},{r["github_count"]},{r["github_exact"]},"{urls_joined}"')
            # Also print the tuple form you asked for
            print((r["name"], r["pypi"], r["conda_forge"], r["github_count"], r["github_top_urls"]))

if __name__ == "__main__":
    main()
