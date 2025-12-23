# ui_app.py
from __future__ import annotations
import json, re, subprocess, sys, sqlite3
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from pydantic import BaseModel, Field

# ----------------------------
# Config
# ----------------------------
COUNTIES_DIR = Path("data/counties")  # one JSON file per state, named by USPS code (e.g., IN.json)
DEFAULT_STATE = "Indiana"             # which state to preselect in the UI

# Mapping of USPS code -> full state name
USPS_TO_NAME = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","DC":"District of Columbia","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois",
    "IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland",
    "MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana",
    "NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York",
    "NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania",
    "RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah",
    "VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"
}
NAME_TO_USPS = {v: k for k, v in USPS_TO_NAME.items()}

@st.cache_data(show_spinner=False)
def load_counties_map_from_dir(dirpath: Path) -> Dict[str, List[str]]:
    """
    Reads all *.json files in data/counties where each file is an array of county names.
    File name must be the state's USPS code (e.g., IN.json). Returns {Full State Name: [counties...]}.
    """
    result: Dict[str, List[str]] = {}
    if not dirpath.exists():
        return result

    for p in sorted(dirpath.glob("*.json")):
        code = p.stem.upper()
        state_name = USPS_TO_NAME.get(code)
        if not state_name:
            continue
        try:
            counties = json.loads(p.read_text(encoding="utf-8"))
            # normalize: strip, title-case, drop “ County” suffix if present
            cleaned = sorted({c.strip().removesuffix(" County").strip().title() for c in counties if isinstance(c, str)})
            if cleaned:
                result[state_name] = cleaned
        except Exception:
            continue
    return result

COUNTIES_BY_STATE = load_counties_map_from_dir(COUNTIES_DIR)
ALL_STATES = sorted(USPS_TO_NAME.values())  # full names for the dropdown

# ----------------------------
# Criteria model (shared contract)
# ----------------------------
class Criteria(BaseModel):
    state: str
    county: str
    max_price_per_acre: Optional[float] = Field(None, gt=0)
    min_acres: Optional[float] = Field(None, gt=0)
    max_acres: Optional[float] = Field(None, gt=0)
    power_nearby: bool = False
    exclude_flood_zone: bool = False
    zoning_whitelist: Optional[List[str]] = None
    report_email: Optional[str] = Field(None, description="Email to send crawl report")

    def dict_clean(self) -> dict:
        return self.model_dump(exclude_none=True)

# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="Property Bot – Search Criteria", page_icon="🗺️", layout="centered")
st.title("🗺️ Property Search Criteria")

# ---------- Location (OUTSIDE the form so it updates instantly) ----------
st.subheader("Location")

# remember last selected state to know when to reset county
if "last_state" not in st.session_state:
    st.session_state.last_state = DEFAULT_STATE if DEFAULT_STATE in ALL_STATES else ALL_STATES[0]
if "county_sel" not in st.session_state:
    st.session_state.county_sel = "(Any)"

# State select
default_state_idx = ALL_STATES.index(st.session_state.last_state) if st.session_state.last_state in ALL_STATES else 0
state = st.selectbox(
    "State",
    options=ALL_STATES,
    index=default_state_idx,
    key="state_sel",
    help="Choose a state to auto-populate the county list from /data/counties."
)

# If state changed, reset county to "(Any)"
if state != st.session_state.last_state:
    st.session_state.county_sel = "(Any)"
    st.session_state.last_state = state

# Counties for selected state
counties = COUNTIES_BY_STATE.get(state, [])
county_options = ["(Any)"] + counties if counties else ["(Any)"]
county = st.selectbox(
    "County",
    options=county_options,
    index=county_options.index(st.session_state.county_sel) if st.session_state.county_sel in county_options else 0,
    key="county_sel",
    help="Type to search the county list."
)

st.divider()

# ---------- Main form ----------
with st.form("criteria_form", clear_on_submit=False):
    st.subheader("Price & Size")
    max_price_per_acre = st.number_input("Max $/acre", min_value=0.0, step=100.0, format="%.2f", value=0.0)
    col1, col2 = st.columns(2)
    with col1:
        min_acres = st.number_input("Min acres (optional)", min_value=0.0, step=1.0, format="%.2f", value=0.0)
    with col2:
        max_acres = st.number_input("Max acres (optional)", min_value=0.0, step=1.0, format="%.2f", value=0.0)

    st.divider()
    st.subheader("Must-Have Checkboxes")
    power_nearby = st.checkbox("Power nearby (meter-ready)", value=False)
    exclude_flood_zone = st.checkbox("Exclude flood zones (FEMA AE/A/VE)", value=True)

    # ----- Zoning (optional) -----
    st.subheader("Zoning (optional)")

    # Common, portable codes + short plain-English summaries.
    ZONING_INFO = {
        "AG": "Agricultural — farming/livestock; low density; often OK for one home & outbuildings",
        "A-1": "Agricultural (A-1) — large-lot/estate ag; very low density",
        "A-2": "Agricultural (A-2) — small-scale/ranchette ag; still low density",
        "RR": "Rural Residential — large lots; typically single-family; some outbuildings allowed",
        "R-1": "Single-Family Residential — one home/lot; smaller lots; tighter setbacks",
        "R-2": "Two-Family/Low-Multi — duplex/low density multi-family",
        "R-3": "Multi-Family — apartments/townhomes; highest residential density",
        "C-1": "Neighborhood Commercial — small shops/services; light traffic",
        "C-2": "General/Community Commercial — broader retail/office; busier corridors",
        "C-3": "Highway/Intensive Commercial — auto-oriented, big box, highway uses",
        "I-1": "Light Industrial — warehousing/assembly; lighter external impacts",
        "I-2": "Heavy Industrial — manufacturing/heavy uses; setbacks/buffers common",
        "MU": "Mixed Use — combo of residential/commercial; town centers/corridors",
        "OS": "Open Space/Conservation — parks, preserves; very limited building",
        "FR": "Forest/Resource — timber/resource lands; very large minimum lots"
    }

    ZONING_CODES = list(ZONING_INFO.keys())

    def zoning_label(code: str) -> str:
        return f"{code} — {ZONING_INFO.get(code, '').strip()}"

    zoning_whitelist = st.multiselect(
        "Include ONLY these zoning types",
        options=ZONING_CODES,
        format_func=zoning_label,
        help="Leave blank to accept any zoning. Always confirm the exact local definition in the county’s code."
    )

    st.divider()
    st.subheader("Delivery")
    report_email = st.text_input(
        "Report email (optional)",
        placeholder="you@example.com",
        help="Stored in criteria.json so the crawler can email you a report."
    )

    submitted = st.form_submit_button("💾 Save criteria")

# ---------- Save handler ----------
if submitted:
    county_clean = None if st.session_state.county_sel in (None, "", "(Any)") else st.session_state.county_sel
    max_ppa_clean = None if max_price_per_acre <= 0 else float(max_price_per_acre)
    min_acres_clean = None if min_acres <= 0 else float(min_acres)
    max_acres_clean = None if max_acres <= 0 else float(max_acres)
    zoning_clean = zoning_whitelist or None

    # email cleanup + basic validation
    email_clean = report_email.strip() if report_email else None
    if email_clean and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email_clean):
        st.error("Please enter a valid email address (or leave it blank).")
        st.stop()

    try:
        payload = Criteria(
            state=st.session_state.state_sel,
            county=county_clean or "(Any)",
            max_price_per_acre=max_ppa_clean,
            min_acres=min_acres_clean,
            max_acres=max_acres_clean,
            power_nearby=power_nearby,
            exclude_flood_zone=exclude_flood_zone,
            zoning_whitelist=zoning_clean,
            report_email=email_clean
        ).dict_clean()

        Path("criteria.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        st.success("Criteria saved to criteria.json")
        st.json(payload)
    except Exception as e:
        st.error(f"Could not save criteria: {e}")

# ---------- Crawler controls (OUTSIDE the form) ----------
st.divider()
st.subheader("Crawler")

colA, colB = st.columns([1, 1])

with colA:
    if st.button("▶️ Run Crawl"):
        if not Path("criteria.json").exists():
            st.error("Please save criteria first.")
        else:
            with st.spinner("Running crawler..."):
                cmd = [sys.executable, "-m", "crawler_service.main", "--criteria", "criteria.json"]
                try:
                    out = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd(), timeout=300)
                    st.code(out.stdout or "(no stdout)")
                    if out.returncode != 0:
                        st.error(out.stderr or "Crawler exited with non-zero status.")
                    else:
                        st.success("Crawler finished.")
                        st.session_state["_refresh_results"] = True
                except Exception as e:
                    st.error(f"Failed to run crawler: {e}")

with colB:
    if st.button("🔄 Refresh Results"):
        st.session_state["_refresh_results"] = True

# ---------- Results viewer (FULL WIDTH) ----------
if st.session_state.get("_refresh_results", True):
    DB_PATH = Path("property_bot.sqlite")
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        try:
            df = pd.read_sql_query("""
                SELECT l.source, l.title, l.url, l.price, l.acres, l.price_per_acre, l.address, l.first_seen, l.last_seen,
                       e.flood_zone, e.zoning_code, e.internet, e.power_hint
                FROM listings l
                LEFT JOIN listing_enrichment e ON e.listing_url = l.url
                ORDER BY l.last_seen DESC
                LIMIT 100
            """, conn)
        finally:
            conn.close()

        st.subheader("Results")
        st.caption("Showing the 100 most recent. Click a URL to open the listing.")
        st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.info("No database yet. Run the crawler to generate listings.")
