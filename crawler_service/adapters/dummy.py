# crawler_service/adapters/dummy.py
from __future__ import annotations
from typing import Iterable, List
from dataclasses import asdict

from .base import SourceAdapter
from ..models import Criteria, Listing

class DummyAdapter(SourceAdapter):
    """
    Test adapter that fabricates a few listings so the pipeline/UI can be exercised.
    Safe to keep enabled while you bring real adapters online.
    """
    name = "dummy"

    def search(self, criteria: Criteria) -> Iterable[Listing]:
        rows: List[Listing] = []
        base_url = "https://example.com/listing/"
        min_acres = criteria.min_acres or 2.0

        for i in range(1, 6):
            acres = min_acres + i * 2
            price = 10000 * acres
            ppa = round(price / acres, 2)

            lst = Listing(
                source=self.name,
                url=f"{base_url}{(criteria.state or 'State').replace(' ', '-')}-"
                    f"{(criteria.county or 'County').replace(' ', '-')}-{i}",
                title=f"{acres:.1f} acres in {criteria.county}, {criteria.state}",
                price=price,
                acres=acres,
                price_per_acre=ppa,
                address=f"{criteria.county}, {criteria.state}",
                lat=None,
                lon=None,
                extras={"badges": ["power? yes" if criteria.power_nearby else "power? unknown"]}
            )
            if self.keep_by_criteria(lst, criteria):
                rows.append(lst)

        # Yield instead of return list (consistent adapter interface)
        for r in rows:
            yield r
