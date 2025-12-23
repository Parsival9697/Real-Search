# crawler_service/enrich/zoning.py
from __future__ import annotations
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

from ..settings import (
    BASE_USER_AGENT,
    REQUEST_TIMEOUT,
    PER_HOST_RPS,
    JITTER_RANGE,
    CONNECT_RETRIES,
)
from ..utils import polite_pause

"""
Zoning lookups vary by county. This module supports:
- A registry you can populate at runtime via register_zoning_source().
- Optional JSON config at data/zoning_sources.json so you don't touch code.

JSON format (array of entries):
[
  {
    "state": "Indiana",
    "county": "Tippecanoe",
    "url": "https://<county-gis>/arcgis/rest/services/Zoning/MapServer",
    "layer_id": 3,
    "field": "ZONE"
  }
]
"""

CONFIG_PATH = Path("data/zoning_sources.json")

@dataclass(frozen=True)
class ZoningSource:
    url: str            # ArcGIS Feature/MapServer base URL (no trailing /<layer>)
    layer_id: int       # layer to query
    field: str          # attribute field that contains the zoning code/name (e.g., "ZONE")

# In-memory registry keyed by ("state lower", "county lower")
_REGISTRY: Dict[Tuple[str, str], ZoningSource] = {}

_session = requests.Session()
_session.headers.update({"User-Agent": BASE_USER_AGENT})

def _host(url: str) -> str:
    return urlparse(url).netloc

def register_zoning_source(state: str, county: str, url: str, layer_id: int, field: str) -> None:
    """Programmatic registration for a county zoning layer."""
    key = (state.strip().lower(), county.strip().lower())
    _REGISTRY[key] = ZoningSource(url=url.strip(), layer_id=int(layer_id), field=field.strip())

def _load_config_file() -> None:
    """Load data/zoning_sources.json if present."""
    if not CONFIG_PATH.exists():
        return
    try:
        items = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        if not isinstance(items, list):
            return
        for it in items:
            st = it.get("state")
            co = it.get("county")
            url = it.get("url")
            lid = it.get("layer_id")
            fld = it.get("field")
            if all([st, co, url]) and isinstance(lid, int) and isinstance(fld, str):
                register_zoning_source(st, co, url, lid, fld)
    except Exception:
        # Best effort; ignore malformed config
        pass

# Load config at import time
_load_config_file()

def _query_arcgis_point(base_url: str, layer_id: int, field: str, lat: float, lon: float) -> Optional[str]:
    """
    ArcGIS REST /query with point geometry in WGS84.
    Works for both MapServer and FeatureServer base URLs.
    """
    base = base_url.rstrip("/")
    if not (base.endswith("/MapServer") or base.endswith("/FeatureServer")):
        base = base + "/MapServer"
    qurl = f"{base}/{layer_id}/query"
    params = {
        "f": "json",
        "geometry": f'{{"x":{lon},"y":{lat},"spatialReference":{{"wkid":4326}}}}',
        "geometryType": "esriGeometryPoint",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "false",
        "outFields": "*",
    }
    host = _host(qurl)
    for attempt in range(CONNECT_RETRIES + 1):
        try:
            polite_pause(host, PER_HOST_RPS, JITTER_RANGE)
            r = _session.get(qurl, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code >= 400:
                return None
            js = r.json()
            feats = js.get("features") or []
            if not feats:
                return None
            attrs = feats[0].get("attributes") or {}
            # Try various casings for the field
            val = attrs.get(field) or attrs.get(field.upper()) or attrs.get(field.lower()) or attrs.get(field.title())
            if isinstance(val, str) and val.strip():
                return val.strip()
            # Sometimes split across different attribute names
            for alt in ("ZONE", "ZONING", "ZONE_CODE", "ZONING_CODE", "ZONE_NAME"):
                if alt.lower() == field.lower():
                    continue
                v = attrs.get(alt) or attrs.get(alt.lower()) or attrs.get(alt.upper()) or attrs.get(alt.title())
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return None
        except requests.RequestException:
            if attempt >= CONNECT_RETRIES:
                return None
    return None

def zoning_for(lat: Optional[float], lon: Optional[float], *, state: Optional[str] = None, county: Optional[str] = None) -> Optional[str]:
    """
    Look up zoning code/name for a point using a county ArcGIS layer if registered.
    - If state/county are provided, we use that key.
    - If not provided, we try all registered sources (first match wins).
    Returns None if no source or no intersecting feature.
    """
    if lat is None or lon is None:
        return None

    # If state & county provided, only check that specific source
    if state and county:
        key = (state.strip().lower(), county.strip().lower())
        src = _REGISTRY.get(key)
        if not src:
            return None
        return _query_arcgis_point(src.url, src.layer_id, src.field, float(lat), float(lon))

    # Otherwise, try all registered sources (first hit wins)
    for src in _REGISTRY.values():
        val = _query_arcgis_point(src.url, src.layer_id, src.field, float(lat), float(lon))
        if val:
            return val
    return None
