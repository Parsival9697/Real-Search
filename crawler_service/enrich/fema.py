# crawler_service/enrich/fema.py
from __future__ import annotations
from functools import lru_cache
from typing import Optional, Tuple, List
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

# FEMA NFHL ArcGIS service (public)
NFHL_MAPSERVER = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer"

_session = requests.Session()
_session.headers.update({"User-Agent": BASE_USER_AGENT})

def _host(url: str) -> str:
    return urlparse(url).netloc

@lru_cache(maxsize=1)
def _candidate_layers() -> List[Tuple[int, str]]:
    """
    Discover NFHL layers that expose a flood zone field.
    Returns a list of (layer_id, field_name) (e.g., (28, 'FLD_ZONE')).
    """
    host = _host(NFHL_MAPSERVER)
    for attempt in range(CONNECT_RETRIES + 1):
        try:
            polite_pause(host, PER_HOST_RPS, JITTER_RANGE)
            resp = _session.get(f"{NFHL_MAPSERVER}?f=json", timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            out: List[Tuple[int, str]] = []
            for lyr in data.get("layers", []):
                if lyr.get("type") != "Feature Layer":
                    continue
                fields = lyr.get("fields", [])
                field_names = {f.get("name", "").upper() for f in fields}
                # Common FEMA fields for zone
                for cand in ("FLD_ZONE", "ZONE", "ZONE_SUBTY", "ZONE_SUBTYPE"):
                    if cand in field_names:
                        out.append((lyr["id"], cand))
                        break
            # De-dup and keep in order
            seen = set()
            uniques = []
            for t in out:
                if t not in seen:
                    uniques.append(t)
                    seen.add(t)
            return uniques
        except requests.RequestException:
            if attempt >= CONNECT_RETRIES:
                return []
    return []

def _query_layer_for_point(layer_id: int, zone_field: str, lat: float, lon: float) -> Optional[str]:
    """
    ArcGIS REST /query using point geometry (WGS84). Returns the zone string or None.
    """
    url = f"{NFHL_MAPSERVER}/{layer_id}/query"
    params = {
        "f": "json",
        "geometry": f'{{"x":{lon},"y":{lat},"spatialReference":{{"wkid":4326}}}}',
        "geometryType": "esriGeometryPoint",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "false",
        "outFields": "*",
    }
    host = _host(url)
    for attempt in range(CONNECT_RETRIES + 1):
        try:
            polite_pause(host, PER_HOST_RPS, JITTER_RANGE)
            r = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code >= 400:
                return None
            js = r.json()
            feats = js.get("features") or []
            if not feats:
                return None
            attrs = feats[0].get("attributes") or {}
            val = attrs.get(zone_field) or attrs.get(zone_field.lower()) or attrs.get(zone_field.title())
            if isinstance(val, str):
                z = val.strip().upper()
                # Normalize some common aliases
                if z in {"X", "ZONE X", "0.2 PCT ANNUAL CHANCE FLOOD HAZARD"}:
                    return "X"
                return z
            return None
        except requests.RequestException:
            if attempt >= CONNECT_RETRIES:
                return None
    return None

def flood_zone_for(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    """
    Look up FEMA NFHL flood zone code (e.g., 'AE', 'A', 'VE', 'X') for a point.
    Returns None if lat/lon are missing or no NFHL feature intersects.
    """
    if lat is None or lon is None:
        return None

    for (layer_id, field_name) in _candidate_layers() or []:
        zone = _query_layer_for_point(layer_id, field_name, float(lat), float(lon))
        if zone:
            return zone
    return None
