# crawler_service/adapters/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Optional

from ..models import Criteria, Listing


class SourceAdapter(ABC):
    """
    Base class for all source adapters.

    Implement `search(self, criteria) -> Iterable[Listing]` to yield normalized
    Listing objects. Adapters should be *polite* (respect RPS) and resilient.

    Use `keep_by_criteria()` to pre-filter obvious non-matches (numeric filters)
    before inserting into the DB. Enrichment-dependent filters (flood, zoning,
    power) are usually applied later once enrichment data exists, so this base
    method only enforces min/max acres and max $/acre. If an adapter *does*
    populate extras like `power_hint` or `zoning_code`, this method will
    respect them without penalizing missing values.
    """

    # Adapter identifier used in DB rows
    name: str = "base"

    @abstractmethod
    def search(self, criteria: Criteria) -> Iterable[Listing]:
        """Yield Listings that (ideally) match the given criteria."""
        raise NotImplementedError

    # ------------------------------
    # Shared filtering helpers
    # ------------------------------
    def keep_by_criteria(self, lst: Listing, c: Criteria) -> bool:
        """Lightweight filter applied before DB insert."""
        # Min/Max acres
        if c.min_acres is not None and lst.acres is not None and lst.acres < c.min_acres:
            return False
        if c.max_acres is not None and lst.acres is not None and lst.acres > c.max_acres:
            return False

        # Max $/acre
        if c.max_price_per_acre is not None:
            # prefer listing.price_per_acre; if absent, compute if possible
            ppa = lst.price_per_acre
            if ppa is None and (lst.price is not None and lst.acres not in (None, 0)):
                ppa = round(float(lst.price) / float(lst.acres), 2)
            if ppa is not None and ppa > c.max_price_per_acre:
                return False

        # Optional early filters if adapter provided hints (never penalize missing)
        if c.power_nearby:
            power_hint = (lst.extras or {}).get("power_hint")
            if power_hint is False:
                return False  # explicit "no power"
        if c.exclude_flood_zone:
            fz = (lst.extras or {}).get("flood_zone")
            if isinstance(fz, str) and fz.strip():
                # basic heuristic: drop common high-risk zones
                if any(k in fz.upper() for k in ("AE", "A ", " VE", "V ")):
                    return False
        if c.zoning_whitelist:
            z = (lst.extras or {}).get("zoning_code")
            if z and isinstance(z, str):
                # keep only if code matches one of the whitelist tokens (case-insensitive)
                z_upper = z.upper()
                if not any(code.upper() in z_upper for code in c.zoning_whitelist):
                    return False

        return True

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={getattr(self, 'name', None)!r}>"
