# crawler_service/adapters/websearch_cse.py
from __future__ import annotations
import os
import time
import random
from typing import Iterable, List, Tuple
from urllib.parse import urlparse

import requests

from .base import SourceAdapter
from ..budget import CrawlBudget
from ..linkcheck import verify_url
from ..models import Criteria, Listing
from ..utils import (
    squish_spaces,
    parse_price,
    parse_acres,
    parse_price_per_acre,
    price_per_acre,
)

class WebSearchCSEAdapter(SourceAdapter):
    """
    Broad discovery via Google Programmable Search (CSE).

    Flow:
      - Build smart queries from State (+ optional County)
      - Call CSE API to get result links + snippets (no direct site scraping here)
      - Score results for likelihood of being a land listing
      - Respect a per-run budget (total + per-host) to avoid hammering
      - Verify each URL (HTTP + content signals) before yielding
      - Filter by numeric criteria (min/max acres, max $/acre)

    Required env:
      GOOGLE_API_KEY        -> your Google API key for Custom Search API
      GOOGLE_CSE_ID         -> your CSE (cx) id

    Optional env:
      CSE_SOURCES           -> comma list of preferred domains (bias score, not a hard allowlist)
      CSE_PER_QUERY         -> items per query (1..10, default 10)
      CSE_SLEEP_SEC         -> polite delay between CSE calls (default 1.0)
      RS_SCORE_THRESHOLD    -> minimum score to consider verifying (default 1.0)
      RS_EXPLORATION_RATE   -> probability to try sub-threshold links anyway (default 0.1)

    Notes:
      - Domain allow/blocking is handled in linkcheck.verify_url (broad mode by default).
    """

    name = "websearch_cse"
    API_URL = "https://www.googleapis.com/customsearch/v1"

    def __init__(self) -> None:
        self.key = os.environ.get("GOOGLE_API_KEY")
        self.cx = os.environ.get("GOOGLE_CSE_ID")
        self.sources = [s.strip() for s in os.environ.get("CSE_SOURCES", "").split(",") if s.strip()]
        self.per_query = int(os.environ.get("CSE_PER_QUERY", "10"))
        self.sleep_sec = float(os.environ.get("CSE_SLEEP_SEC", "1.0"))
        self.score_threshold = float(os.environ.get("RS_SCORE_THRESHOLD", "1.0"))
        self.explore_rate = float(os.environ.get("RS_EXPLORATION_RATE", "0.1"))

    # ---------------- internals ----------------

    def _enabled(self) -> bool:
        if not (self.key and self.cx):
            print("[cse] GOOGLE_API_KEY or GOOGLE_CSE_ID not set; adapter disabled.")
            return False
        return True

    def _queries(self, c: Criteria) -> List[str]:
        """
        Construct 1..N query strings. If preferred sources are configured, we build
        one query per domain; otherwise, one broad query for the whole web.
        """
        state = (c.state or "").strip()
        county = (c.county_normalized or "").strip()

        locs: List[str] = []
        if county:
            # Try both "County, State" and "County, State" without the word 'County'
            locs += [f'"{county} County, {state}"', f'"{county}, {state}"']
        else:
            locs += [f'"{state}"']

        constraints = ['"land for sale"', 'acre OR acres']
        base = " ".join(locs + constraints)

        if self.sources:
            return [f"site:{dom} {base}" for dom in self.sources]
        return [base]

    def _call_cse(self, q: str) -> List[dict]:
        params = {
            "key": self.key,
            "cx": self.cx,
            "q": q,
            "num": min(max(self.per_query, 1), 10),
            "safe": "off",
        }
        try:
            r = requests.get(self.API_URL, params=params, timeout=20)
        except requests.RequestException as e:
            print(f"[cse] request error for query: {q} -> {e}")
            return []
        if r.status_code != 200:
            print(f"[cse] HTTP {r.status_code} for query: {q}")
            return []
        return r.json().get("items") or []

    @staticmethod
    def _score_item(title: str, snippet: str, domain: str, c: Criteria) -> float:
        """
        Heuristic score for how listing-like a CSE item appears.
        """
        t = (title or "").lower()
        s = (snippet or "").lower()
        score = 0.0

        # strong signals
        if "land for sale" in s or "land for sale" in t:
            score += 3
        if "acre" in s or "acre" in t:
            score += 2
        if "$" in s or "$" in t:
            score += 2
        if "mls" in s or "apn" in s or "parcel" in s or "lot size" in s:
            score += 1.5

        # location alignment
        st = (c.state or "").lower()
        co = (c.county_normalized or "").lower()
        if st and (st in s or st in t):
            score += 1
        if co and (co in s or co in t):
            score += 1.5

        # numeric hints aligned to criteria
        p = parse_price(snippet)
        ac = parse_acres(snippet)
        ppa = parse_price_per_acre(snippet)
        if ppa is not None and c.max_price_per_acre and ppa <= c.max_price_per_acre:
            score += 3
        if ac is not None:
            if c.min_acres and ac >= c.min_acres:
                score += 1
            if c.max_acres and ac <= c.max_acres:
                score += 1

        # slight preference for user-preferred sources
        preferred = [s.strip().lower() for s in os.environ.get("CSE_SOURCES", "").split(",") if s.strip()]
        if preferred and any(domain.endswith(d) for d in preferred):
            score += 0.5

        return score

    # ---------------- public API ----------------

    def search(self, criteria: Criteria) -> Iterable[Listing]:
        """
        Yield Listings that pass numeric filters (keep_by_criteria in base class).
        Uses a fresh CrawlBudget per call (typically per state).
        """
        if not self._enabled():
            return

        budget = CrawlBudget()
        seen_urls = set()

        for q in self._queries(criteria):
            if getattr(budget, "exhausted", False):
                print("[cse] budget exhausted for this state; stopping.")
                break

            print(f"[cse] {q}", flush=True)
            items = self._call_cse(q)
            if not items:
                time.sleep(self.sleep_sec)
                continue

            # Build a scored frontier
            scored: List[Tuple[float, dict]] = []
            for it in items:
                url = it.get("link") or it.get("formattedUrl")
                if not url:
                    continue
                u = url.strip()
                if not u or u.lower() in seen_urls:
                    continue
                seen_urls.add(u.lower())

                title = squish_spaces(it.get("title") or "") or ""
                snippet = squish_spaces(it.get("snippet") or "") or ""
                domain = urlparse(u).netloc.lower()
                s = self._score_item(title, snippet, domain, criteria)
                scored.append((s, it))

            # Highest score first
            scored.sort(key=lambda x: x[0], reverse=True)

            for s, it in scored:
                url = it.get("link") or it.get("formattedUrl")
                if not url:
                    continue
                url = url.strip()
                if not url:
                    continue

                # Gate by score + exploration
                if s < self.score_threshold and random.random() > self.explore_rate:
                    continue

                # Respect per-run budget (reserve before network)
                if not budget.consume(url):
                    print("[cse] budget exhausted; stopping this query.")
                    break

                # Verify the URL (HTTP + content signals)
                ck = verify_url(url)
                if not ck.ok:
                    print(f"[cse] drop {url} -> {ck.reason} (status {ck.status})", flush=True)
                    continue

                # Build Listing using verified hints first; fall back to snippet
                title = squish_spaces(it.get("title") or "") or None
                snippet = squish_spaces(it.get("snippet") or "") or ""
                p = ck.price or parse_price(snippet)
                ac = ck.acres or parse_acres(snippet)
                ppa = ck.price_per_acre or parse_price_per_acre(snippet) or price_per_acre(p, ac)

                lst = Listing(
                    source=self.name,
                    url=ck.canonical_url or url,
                    title=title,
                    price=p,
                    acres=ac,
                    price_per_acre=ppa,
                    extras={"query": q, "ctype": ck.content_type or "", "score": s},
                )

                if self.keep_by_criteria(lst, criteria):
                    yield lst

            time.sleep(self.sleep_sec)
