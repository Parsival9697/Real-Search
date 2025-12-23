# crawler_service/adapters/landlike.py
from __future__ import annotations
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .base import SourceAdapter
from ..models import Criteria, Listing
from ..utils import parse_price, parse_acres, price_per_acre, polite_pause
from ..settings import (
    BASE_USER_AGENT,
    REQUEST_TIMEOUT,
    PER_HOST_RPS,
    JITTER_RANGE,
    DEFAULT_PAGE_CAP,
    CONNECT_RETRIES,
)

class LandLikeAdapter(SourceAdapter):
    """
    Example 'land-style' adapter to crawl public listing pages.
    >>> IMPORTANT <<<
    - Update BASE_URL to the site you’re targeting.
    - Update build_search_url() to match that site’s public search pattern.
    - Update selectors in _parse_cards() and _next_page_url() to match the DOM.
    """

    name = "landlike"
    BASE_URL = "https://www.landwatch.com/"  # <-- CHANGE for your target site

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": BASE_USER_AGENT})

    # ---------------- URL construction (adjust per site) ----------------
    def build_search_url(self, c: Criteria, page: int = 1) -> str:
        """
        Example pattern often seen on land portals. You must tailor this
        to the actual site's URL scheme.
        """
        # e.g., "Indiana" -> "Indiana", "Tippecanoe" -> "Tippecanoe-County"
        state_slug = (c.state or "").strip().replace(" ", "-")
        county_slug = ""
        if c.county and c.county not in ("(Any)", ""):
            county_slug = f"{c.county.strip().replace(' ', '-')}-County"

        # Example path: /Indiana-Land-for-sale/Tippecanoe-County
        path = f"{state_slug}-Land-for-sale/"
        path = path + county_slug if county_slug else path

        url = urljoin(self.BASE_URL, path)
        if page > 1:
            url = f"{url}?page={page}"
        return url

    def _host(self) -> str:
        return urlparse(self.BASE_URL).netloc

    # ---------------- Fetch with throttle/retry ----------------
    def _get(self, url: str) -> Optional[BeautifulSoup]:
        host = self._host()
        for attempt in range(1, CONNECT_RETRIES + 2):
            try:
                polite_pause(host, PER_HOST_RPS, JITTER_RANGE)
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if r.status_code >= 400:
                    return None
                return BeautifulSoup(r.text, "lxml")
            except requests.RequestException:
                if attempt >= CONNECT_RETRIES + 1:
                    return None
        return None

    # ---------------- Parse a list/search page ----------------
    def _parse_cards(self, soup: BeautifulSoup) -> List[Listing]:
        """
        Adjust selectors for your target site.
        Generic fallbacks try to find card-like containers.
        """
        listings: List[Listing] = []

        # Try a few common patterns
        cards = (
            soup.select("article")
            or soup.select("div.result, li.result")
            or soup.select("div.card")
            or soup.select("div.search-result, div.listing")
        )

        for card in cards:
            # Link + title
            a = card.select_one("a[href]")
            if not a:
                continue
            url = a.get("href")
            if url and not url.startswith("http"):
                url = urljoin(self.BASE_URL, url)
            title = (a.get_text(strip=True) or "Land")

            # Pull text to extract price/acres heuristically
            text_chunks = " ".join(t.get_text(" ", strip=True) for t in card.select("div, span, p"))
            price = parse_price(text_chunks)
            acres = parse_acres(text_chunks)
            ppa = price_per_acre(price, acres)

            listings.append(Listing(
                source=self.name,
                url=url or "",
                title=title,
                price=price,
                acres=acres,
                price_per_acre=ppa,
                address=None,
                lat=None,
                lon=None,
                extras={"raw": text_chunks[:500]}
            ))

        return listings

    def _next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """
        Adjust for each site: look for a 'Next' pagination link.
        """
        next_a = soup.select_one("a[rel=next], a.next, a.pagination__next, li.next > a")
        if next_a and next_a.get("href"):
            href = next_a["href"]
            return href if href.startswith("http") else urljoin(current_url, href)
        return None

    # ---------------- Public: iterate pages & yield listings ----------------
    def search(self, criteria: Criteria) -> Iterable[Listing]:
        seen_urls = set()
        page = 1
        while page <= DEFAULT_PAGE_CAP:
            url = self.build_search_url(criteria, page=page)
            soup = self._get(url)
            if not soup:
                break

            batch = self._parse_cards(soup)
            if not batch:
                break

            for lst in batch:
                if not lst.url or lst.url in seen_urls:
                    continue
                seen_urls.add(lst.url)

                if not self.keep_by_criteria(lst, criteria):
                    continue

                yield lst

            next_url = self._next_page_url(soup, url)
            if not next_url:
                break
            page += 1
