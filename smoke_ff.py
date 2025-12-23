# smoke_ff.py
from __future__ import annotations
import json, os, sys, time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FFService

ROOT = Path(__file__).resolve().parent
DEBUG = ROOT / "debug"
DEBUG.mkdir(exist_ok=True)

def _log(msg: str) -> None:
    print(msg, flush=True)

def _make_options() -> webdriver.FirefoxOptions:
    opts = webdriver.FirefoxOptions()
    # Headless toggle via env
    headless = str(os.environ.get("FF_HEADLESS", "0")).strip().lower() in {"1","true","yes","on"}
    if headless:
        opts.add_argument("--headless")

    # Force sane client-like prefs
    ua = os.environ.get("FF_UA") or os.environ.get("ZILLOW_UA") or \
         "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0"
    lang = os.environ.get("ZILLOW_ACCEPT_LANG", "en-US,en;q=0.9")
    try:
        opts.set_preference("general.useragent.override", ua)
        opts.set_preference("intl.accept_languages", lang)
        opts.set_preference("network.cookie.cookieBehavior", 0)  # allow cookies
        opts.set_preference("privacy.trackingprotection.enabled", False)
        opts.set_preference("dom.webdriver.enabled", False)
    except Exception:
        pass

    # Optional explicit Firefox binary
    ff_bin = os.environ.get("FIREFOX_BIN", "").strip()
    if ff_bin:
        opts.binary_location = ff_bin

    # Optional -no-remote if you want to test it
    if str(os.environ.get("FF_USE_NO_REMOTE", "0")).strip().lower() in {"1","true","yes","on"}:
        opts.add_argument("-no-remote")
        opts.add_argument("-new-instance")

    return opts

def main() -> int:
    gecko_log = (DEBUG / "geckodriver_smoke_ff.log").resolve()
    service = FFService(log_output=str(gecko_log))

    opts = _make_options()
    _log(f"Geckodriver log -> {gecko_log}")
    drv = webdriver.Firefox(options=opts, service=service)

    try:
        drv.set_page_load_timeout(45)
        drv.set_window_size(1280, 900)

        # 1) JS UA
        js_ua = drv.execute_script("return navigator.userAgent || null;")

        # 2) httpbin headers + user-agent
        drv.get("https://httpbin.org/headers")
        try:
            WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
            pre_text = drv.find_element(By.TAG_NAME, "pre").text
            headers_json = json.loads(pre_text)
        except Exception:
            headers_json = {}

        drv.get("https://httpbin.org/user-agent")
        try:
            WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
            ua_text = drv.find_element(By.TAG_NAME, "pre").text
            ua_json = json.loads(ua_text)
        except Exception:
            ua_json = {}

        # Write artifacts
        (DEBUG / "smoke_user_agent.json").write_text(
            json.dumps({
                "js_navigator_userAgent": js_ua,
                "httpbin_user_agent": ua_json.get("user-agent"),
            }, indent=2),
            encoding="utf-8",
        )
        (DEBUG / "smoke_headers_full.json").write_text(
            json.dumps(headers_json, indent=2), encoding="utf-8"
        )
        (DEBUG / "smoke_headers.json").write_text(
            json.dumps(headers_json.get("headers") or {}, indent=2), encoding="utf-8"
        )

        _log(f"Wrote: {DEBUG / 'smoke_user_agent.json'}")
        _log(f"Wrote: {DEBUG / 'smoke_headers_full.json'}")
        _log(f"Wrote: {DEBUG / 'smoke_headers.json'}")
        _log(f"navigator.userAgent: {js_ua}")
        _log(f"httpbin user-agent: {ua_json.get('user-agent')}")

        return 0
    finally:
        try:
            drv.quit()
        except Exception:
            pass

if __name__ == "__main__":
    raise SystemExit(main())
