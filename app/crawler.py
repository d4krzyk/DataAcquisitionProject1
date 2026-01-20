import os
import time
import queue
import re
import requests
from urllib.parse import urljoin, urlparse
import urllib.robotparser
from bs4 import BeautifulSoup

from limits import RateLimitItemPerSecond
from limits.storage import RedisStorage
from limits.strategies import FixedWindowRateLimiter

from database import create_table, insert_document


def _env(name: str, default=None):
    v = os.getenv(name)
    return v if v not in (None, "") else default


HTTP_USER_AGENT = _env(
    "CRAWLER_USER_AGENT",
    "index-searcher/1.0 (+https://example.com/botinfo)",
)

ROBOTS_AGENT = _env("CRAWLER_ROBOTS_AGENT", "*")

START_URL = _env("CRAWLER_START_URL", "https://pl.wikipedia.org/wiki/Toru%C5%84")
MAX_DEPTH = int(_env("CRAWLER_MAX_DEPTH", 2))
DELAY_DEFAULT = float(_env("CRAWLER_DELAY", 1.0))
ALLOWED_DOMAIN = _env("CRAWLER_ALLOWED_DOMAIN", "pl.wikipedia.org")

# --- Rate limiting (limits + Redis) ---
REDIS_URL = _env("CRAWLER_REDIS_URL", "redis://redis:6379")
RATE_PER_SEC = int(_env("CRAWLER_RATE_PER_SEC", 10))
RATE_KEY = _env("CRAWLER_RATE_KEY", "wiki_crawler")  # wspólny klucz limitu

redis_storage = RedisStorage(REDIS_URL)
limiter = FixedWindowRateLimiter(redis_storage)
rate = RateLimitItemPerSecond(RATE_PER_SEC)


def is_allowed(key: str) -> bool:
    return bool(limiter.hit(rate, key))


def _wait_for_rate_limit(key: str):
    while not is_allowed(key):
        time.sleep(0.05)

LINK_PATH_REGEX = _env("CRAWLER_LINK_PATH_REGEX", r"^/wiki/[^:#]*$")
LINK_PATH_RE = re.compile(LINK_PATH_REGEX)


def canonicalize(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}"


def is_allowed_link(href: str) -> bool:
    if not href:
        return False
    parsed = urlparse(href)
    if parsed.netloc and ALLOWED_DOMAIN not in parsed.netloc:
        return False
    return bool(LINK_PATH_RE.match(parsed.path))


def _robots_url_for(any_url: str) -> str:
    p = urlparse(any_url)
    return f"{p.scheme}://{p.netloc}/robots.txt"


def get_robots_parser(any_url: str, session: requests.Session):
    robots_url = _robots_url_for(any_url)
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)

    try:
        _wait_for_rate_limit(f"{RATE_KEY}:robots")
        resp = session.get(
            robots_url,
            headers={"User-Agent": HTTP_USER_AGENT},
            timeout=15,
            allow_redirects=True,
        )
        resp.raise_for_status()
        rp.parse(resp.text.splitlines())
        return rp
    except Exception as e:
        print("robots.txt fetch/parse failed:", robots_url, e)
        return None


def fetch(url: str, session: requests.Session) -> str:
    _wait_for_rate_limit(f"{RATE_KEY}:fetch")
    r = session.get(url, headers={"User-Agent": HTTP_USER_AGENT}, timeout=10)
    r.raise_for_status()
    return r.text


def extract_text_and_links(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "meta", "header", "footer"]):
        tag.decompose()

    # Najpierw zbieranie linków z elementów <a>
    links = set()
    anchors = soup.find_all("a", href=True)
    for a in anchors:
        href = a.get("href")
        if not href or href.startswith("#"):
            continue
        if href.startswith("//"):
            href = "https:" + href
        full = urljoin(base_url, href)
        if is_allowed_link(full):
            links.add(canonicalize(full))

    # Usuwanie elementów <a>, żeby ich tekst nie pojawił się w main text
    for a in anchors:
        a.decompose()

    text = soup.get_text(separator=" ", strip=True)
    return text, links


def save_to_db(url: str, text: str) -> bool:
    try:
        insert_document(url, text or "")
        return True
    except Exception as e:
        print("DB insert failed:", e)
        return False


def _robots_can_fetch(rp, url: str) -> bool:
    if rp is None:
        return False
    try:
        return bool(rp.can_fetch(ROBOTS_AGENT, url))
    except Exception:
        return False


def _robots_delay(rp) -> float:
    if rp is None:
        return DELAY_DEFAULT
    try:
        cd = rp.crawl_delay(ROBOTS_AGENT)
        return float(cd) if cd is not None else DELAY_DEFAULT
    except Exception:
        return DELAY_DEFAULT


def crawl(start_url: str, max_depth: int):
    sess = requests.Session()
    rp = get_robots_parser(start_url, sess)

    start_url_c = canonicalize(start_url)
    if not _robots_can_fetch(rp, start_url_c):
        print("Start URL blocked by robots.txt:", start_url_c)
        print("Robots agent used for matching:", ROBOTS_AGENT)
        print("HTTP User-Agent:", HTTP_USER_AGENT)
        print("Robots URL:", _robots_url_for(start_url_c))
        return

    crawl_delay = _robots_delay(rp)

    visited = set()
    q = queue.Queue()
    q.put((start_url_c, 0))
    last_request = 0.0

    while not q.empty():
        url, depth = q.get()
        if url in visited:
            continue
        visited.add(url)

        if not _robots_can_fetch(rp, url):
            continue

        since = time.time() - last_request
        if since < crawl_delay:
            time.sleep(crawl_delay - since)

        try:
            html = fetch(url, sess)
            last_request = time.time()
        except Exception as e:
            print("Fetch error:", url, e)
            continue

        text, links = extract_text_and_links(html, url)
        saved = save_to_db(url, text)
        print(f"[{depth}] {url} -> saved={saved} links={len(links)}")

        if depth < max_depth:
            for link in links:
                if link not in visited:
                    q.put((link, depth + 1))


if __name__ == "__main__":
    create_table()
    crawl(START_URL, MAX_DEPTH)