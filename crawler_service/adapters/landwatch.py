from __future__ import annotations
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .base import SourceAdapter
from ..models import Criteria, Listing
from ..utils import (
    parse_price, parse_acres, parse_price_per_acre, price_per_acre,
    polite_pause, squish_spaces,
)
from ..settings import (
    default_headers, REQUEST_TIMEOUT, PER_HOST_RPS, JITTER_RANGE,
    DEFAULT_PAGE_CAP, CONNECT_RETRIES,
)

class LandWatchAdapter(SourceAdapter):
    """
    More resilient 'LandWatch-style' adapter.
    - Tries multiple URL patterns (hyphens and underscores; with/without county).
    - Uses broader card selectors.
    - Adds verbose logs so you can see what's happening.
    """
    name = "landwatch"
    BASE_URL = "https://www.landwatch.com/"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.clear()
        self.session.headers.update(default_headers())

    # ---------------- helpers ----------------
    @staticmethod
    def _hy(s: str) -> str:
        return s.strip().replace(" ", "-")

    @staticmethod
    def _us(s: str) -> str:
        return s.strip().replace(" ", "_")

    def _host(self) -> str:
        return urlparse(self.BASE_URL).netloc

    def _candidate_urls(self, c: Criteria, page: int) -> List[str]:
        """Generate several likely search URLs; first that yields cards wins."""
        state = (c.state or "").strip()
        county = (c.county or "").strip()
        county_any = (not county) or (county == "(Any)")

        urls: List[str] = []

        # 1) Hyphen slugs, with/without county
        base_hy = f"{self._hy(state)}-Land-for-sale/"
        if not county_any:
            urls.append(urljoin(self.BASE_URL, f"{base_hy}{self._hy(county)}-County"))
        urls.append(urljoin(self.BASE_URL, base_hy))

        # 2) Underscore slugs (legacy)
        base_us = f"{self._us(state)}_land_for_sale/"
        if not county_any:
            urls.append(urljoin(self.BASE_URL, f"{base_us}{self._us(county)}_County"))
        urls.append(urljoin(self.BASE_URL, base_us))

        # add page query when page > 1
        if page > 1:
            urls = [u + (("&" if "?" in u else "?") + f"page={page}") for u in urls]

        # de-dupe preserving order
        seen = set()
        out = []
        for u in urls:
            if u not in seen:
                seen.add(u); out.append(u)
        return out

    # ---------------- fetch ----------------
    def _get(self, url: str) -> Optional[BeautifulSoup]:
        host = self._host()
        for attempt in range(1, CONNECT_RETRIES + 2):
            try:
                polite_pause(host, PER_HOST_RPS, JITTER_RANGE)
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                print(f"[landwatch] GET {r.status_code} {url}", flush=True)
                if r.status_code >= 400:
                    return None
                return BeautifulSoup(r.text, "lxml")
            except requests.RequestException as e:
                print(f"[landwatch] request error (attempt {attempt}): {e}", flush=True)
                if attempt >= CONNECT_RETRIES + 1:
                    return None
        return None

    # ---------------- parse ----------------
    def _find_cards(self, soup: BeautifulSoup):
        # Try a bunch of reasonable containers
        cards = soup.select("article")
        if not cards: cards = soup.select("div.card, li.card")
        if not cards: cards = soup.select("div.result, li.result")
        if not cards: cards = soup.select("div.search-result, div.listing, li.search-result")
        return cards

    def _parse_cards(self, soup: BeautifulSoup) -> List[Listing]:
        listings: List[Listing] = []
        cards = self._find_cards(soup)
        for card in cards:
            # Link + title
            a = (card.select_one("a[data-testid='listing-title'], a[data-testid='listing-title-link'], a.listing-name")
                 or card.select_one("a[href]"))
            if not a:
                continue
            url = a.get("href", "")
            if url and not url.startswith("http"):
                url = urljoin(self.BASE_URL, url)
            title = squish_spaces(a.get_text(" ", strip=True)) or "Land"

            blob = " ".join(el.get_text(" ", strip=True) for el in card.select("div, span, p, li"))
            blob = squish_spaces(blob) or ""

            ppa = parse_price_per_acre(blob)
            price = parse_price(blob)
            acres = parse_acres(blob)
            if ppa is None:
                ppa = price_per_acre(price, acres)

            listings.append(Listing(
                source=self.name,
                url=url,
                title=title,
                price=price,
                acres=acres,
                price_per_acre=ppa,
                address=None,
                lat=None,
                lon=None,
                extras={"raw": blob[:800]},
            ))
        return listings

    def _next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        next_a = soup.select_one("a[rel='next'], a[aria-label*='Next'], a.next, li.next > a, button[aria-label*='Next']")
        if next_a and next_a.get("href"):
            href = next_a["href"]
            return href if href.startswith("http") else urljoin(current_url, href)
        return None

    # ---------------- public ----------------
    def search(self, criteria: Criteria) -> Iterable[Listing]:
        seen = set()
        for page in range(1, DEFAULT_PAGE_CAP + 1):
            # try multiple URL patterns until one gives cards
            soup = None
            used_url = None
            for url in self._candidate_urls(criteria, page):
                print(f"[landwatch] page {page} try: {url}", flush=True)
                s = self._get(url)
                if not s:
                    continue
                cards = self._find_cards(s)
                if cards:
                    soup = s
                    used_url = url
                    break
            if not soup:
                print(f"[landwatch] page {page}: no candidate URL produced cards", flush=True)
                break

            print(f"[landwatch] parsing cards from {used_url}", flush=True)
            batch = self._parse_cards(soup)
            if not batch:
                print(f"[landwatch] page {page}: 0 parsed cards", flush=True)
                break

            produced = 0
            for lst in batch:
                if not lst.url or lst.url in seen:
                    continue
                if not self.keep_by_criteria(lst, criteria):
                    continue
                seen.add(lst.url)
                produced += 1
                yield lst

            if produced == 0:
                print(f"[landwatch] page {page}: all cards filtered or duplicates", flush=True)
                # still try next page once
            nxt = self._next_page_url(soup, used_url or "")
            if not nxt:
                break
