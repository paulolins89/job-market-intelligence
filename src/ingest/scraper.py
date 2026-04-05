"""
Scraper for hiring.cafe job listings.

Uses a real Firefox session (with the user's existing browser profile) to bypass
Cloudflare's managed challenge. All API calls are made via fetch() from within
the browser context, so CF cookies and TLS fingerprint are always correct.

Fetches jobs for configured search queries in the Netherlands,
saving each run as a timestamped JSON file in data/raw/.
"""

import base64
import json
import logging
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SEARCH_QUERIES = ["data analyst", "business analyst", "python developer"]

FILTER_TEMPLATE = {
    "searchQuery": "",
    "locations": [{"country": "Netherlands"}],
    "dateFetchedPastNDays": 121,
    "workplaceTypes": ["Remote", "Hybrid", "Onsite"],
    "commitmentTypes": ["Full Time", "Part Time", "Contract", "Internship"],
}

API_URL = "https://hiring.cafe/api/search-jobs"
PAGE_SIZE = 40
REQUEST_DELAY = 1.5  # seconds between page requests
MAX_PAGES = None       # Set to an int to cap pages per query (None = unlimited)

FIREFOX_PROFILE_SRC = Path.home() / "snap/firefox/common/.mozilla/firefox/0hio75gx.default"
GECKODRIVER_PATH = "/snap/bin/geckodriver"

RAW_DIR = Path(__file__).parents[2] / "data" / "raw"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Browser session management
# ---------------------------------------------------------------------------

def _copy_profile(src: Path) -> Path:
    """Copy the Firefox profile to a temp dir and remove lock files."""
    tmp = Path(tempfile.mkdtemp(prefix="ff_profile_"))
    shutil.copytree(src, tmp, dirs_exist_ok=True, symlinks=False, ignore_dangling_symlinks=True)
    for lock in ("lock", ".lock", "parent.lock"):
        lp = tmp / lock
        if lp.exists() or lp.is_symlink():
            lp.unlink()
    return tmp


def create_driver(profile_src: Path = FIREFOX_PROFILE_SRC) -> tuple[webdriver.Firefox, Path]:
    """Start a headless Firefox with a copy of the existing profile."""
    tmp_profile = _copy_profile(profile_src)
    opts = Options()
    opts.add_argument("-headless")
    opts.profile = str(tmp_profile)
    service = Service(GECKODRIVER_PATH)
    driver = webdriver.Firefox(options=opts, service=service)
    log.info("Firefox started (profile: %s)", tmp_profile)
    return driver, tmp_profile


def prime_session(driver: webdriver.Firefox) -> None:
    """Visit the site so CF clearance cookie is active in this session."""
    log.info("Priming session via homepage...")
    driver.get("https://hiring.cafe/")
    time.sleep(2)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

_FETCH_SCRIPT = """
const [url, done] = [arguments[0], arguments[1]];
fetch(url, {credentials: 'include'})
    .then(r => {
        if (!r.ok) { done({error: r.status + ' ' + r.statusText}); return; }
        return r.json();
    })
    .then(data => done({results: data.results || []}))
    .catch(e => done({error: e.toString()}));
"""


def fetch_page(driver: webdriver.Firefox, query: str, page: int) -> list[dict]:
    filters = {**FILTER_TEMPLATE, "searchQuery": query}
    s = base64.b64encode(json.dumps(filters, separators=(",", ":")).encode()).decode()
    url = f"{API_URL}?s={s}&size={PAGE_SIZE}&page={page}&sv=control"

    result = driver.execute_async_script(_FETCH_SCRIPT, url)

    if "error" in result:
        raise RuntimeError(f"API error: {result['error']}")

    return result.get("results", [])


def fetch_all_jobs(driver: webdriver.Firefox, query: str) -> list[dict]:
    jobs: list[dict] = []
    page = 0

    while True:
        log.info("  query=%r  page=%d  fetched=%d", query, page, len(jobs))
        results = fetch_page(driver, query, page)

        if not results:
            log.info("  No more results for %r at page %d", query, page)
            break

        jobs.extend(results)

        if len(results) < PAGE_SIZE:
            break

        page += 1
        if MAX_PAGES is not None and page >= MAX_PAGES:
            log.info("  Reached MAX_PAGES=%d for %r", MAX_PAGES, query)
            break

        time.sleep(REQUEST_DELAY)

    return jobs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    driver, tmp_profile = create_driver()
    try:
        prime_session(driver)

        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        total_records = 0

        for query in SEARCH_QUERIES:
            log.info("Fetching: %r", query)
            try:
                jobs = fetch_all_jobs(driver, query)
            except RuntimeError as exc:
                log.error("Failed for %r: %s", query, exc)
                continue

            slug = query.replace(" ", "_")
            out_path = RAW_DIR / f"{run_ts}_{slug}.json"
            payload = {
                "query": query,
                "fetched_at": run_ts,
                "total_results": len(jobs),
                "jobs": jobs,
            }
            out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
            log.info("  Saved %d jobs -> %s", len(jobs), out_path.name)
            total_records += len(jobs)

            time.sleep(REQUEST_DELAY)

        log.info("Run complete. Total records: %d", total_records)
        if total_records < 500:
            log.warning(
                "Only %d records — below the 500-record target. "
                "Consider widening dateFetchedPastNDays or adding more queries.",
                total_records,
            )

    finally:
        driver.quit()
        shutil.rmtree(tmp_profile, ignore_errors=True)
        log.info("Browser closed, temp profile cleaned up.")


if __name__ == "__main__":
    main()
