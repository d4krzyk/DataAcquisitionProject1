
import sys
import types
import urllib.parse
import pytest

# "fałszywe" moduły limits

fake_limits = types.ModuleType("limits")
fake_limits.RateLimitItemPerSecond = lambda x: None
sys.modules["limits"] = fake_limits

fake_limits_storage = types.ModuleType("limits.storage")
class _FakeRedisStorage:
    def __init__(self, url):
        self.url = url
fake_limits_storage.RedisStorage = _FakeRedisStorage
sys.modules["limits.storage"] = fake_limits_storage

fake_limits_strategies = types.ModuleType("limits.strategies")
class _FakeLimiter:
    def __init__(self, storage):
        self.storage = storage
    def hit(self, rate, key):
        return True
fake_limits_strategies.FixedWindowRateLimiter = _FakeLimiter
sys.modules["limits.strategies"] = fake_limits_strategies

import crawler

# testowa domena
TEST_DOMAIN = "pl.wikipedia.org"
crawler.ALLOWED_DOMAIN = TEST_DOMAIN

def test_canonicalize_removes_query_and_fragment():
    url = f"https://{TEST_DOMAIN}/wiki/Toru%C5%84?foo=1#frag"
    got = crawler.canonicalize(url)
    assert got == f"https://{TEST_DOMAIN}/wiki/Toru%C5%84"

def test_robots_url_for():
    any_url = f"https://{TEST_DOMAIN}/wiki/Toru%C5%84"
    assert crawler._robots_url_for(any_url) == f"https://{TEST_DOMAIN}/robots.txt"


def test_is_link_various():
    assert crawler.is_allowed_link("/wiki/Coś") is True
    assert crawler.is_allowed_link(f"https://{TEST_DOMAIN}/wiki/Coś") is True
    # inna domena -> False
    assert crawler.is_allowed_link("https://other.example/wiki/Coś") is False
    # fragment/anchor only
    assert crawler.is_allowed_link("#section") is False
    assert crawler.is_allowed_link(None) is False


def test_extract_text_and_links_filters_and_canonicalizes():
    html = f'''
    <html>
      <head><title>Test</title><script>var a = 1;</script><style>p{{}}</style></head>
      <body>
        <p>Tu jest treść. </p>
        <a href="/wiki/One">One</a>
        <a href="https://{TEST_DOMAIN}/wiki/Two#sec">Two</a>
        <a href="https://external.com/wiki/Three">Other</a>
      </body>
    </html>
    '''
    text, links = crawler.extract_text_and_links(html, f"https://{TEST_DOMAIN}/wiki/Toru%C5%84")
    assert "Tu jest treść" in text
    assert f"https://{TEST_DOMAIN}/wiki/One" in links
    assert f"https://{TEST_DOMAIN}/wiki/Two" in links
    assert all(l.startswith(f"https://{TEST_DOMAIN}/wiki/") for l in links)
    assert not any("external.com" in l for l in links)

def test_crawl_process_adds_start_and_link(monkeypatch):
    class FakeRP:
        def can_fetch(self, agent, url):
            return True
        def crawl_delay(self, agent):
            return None

    monkeypatch.setattr(crawler, 'get_robots_parser', lambda start_url, session: FakeRP())

    def fake_fetch(url, session):
        return '<html><body><p>Some text</p><a href="/wiki/Link1">L</a></body></html>'
    monkeypatch.setattr(crawler, 'fetch', fake_fetch)

    monkeypatch.setattr(crawler, '_wait_for_rate_limit', lambda key: None)

    saved = []
    def fake_save(url, text):
        saved.append((crawler.canonicalize(url), (text or "")))
        return True
    monkeypatch.setattr(crawler, 'save_to_db', fake_save)

    start = f"https://{TEST_DOMAIN}/wiki/Toru%C5%84"
    crawler.crawl(start, max_depth=1)

    expected_start = crawler.canonicalize(start)
    expected_link = crawler.canonicalize(f"https://{TEST_DOMAIN}/wiki/Link1")

    assert (expected_start, "Some text") in saved
    assert (expected_link, "Some text") in saved
    assert len(saved) >= 2