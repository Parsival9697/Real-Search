# smoke_zillow.py
from pathlib import Path
import os, sys
from crawler_service.models import Criteria
from crawler_service.adapters.zillow_selenium import ZillowSeleniumAdapter

# Debug-friendly env defaults for this smoke
os.environ.setdefault("ZILLOW_ALLOWED", "1")
os.environ.setdefault("ZILLOW_DEBUG", "1")
os.environ.setdefault("ZILLOW_KEEP_OPEN_ON_ERROR", "0")
os.environ.setdefault("ZILLOW_HEADLESS", "0")
os.environ.setdefault("ZILLOW_USE_NO_REMOTE", "1")
os.environ.setdefault("ZILLOW_WAIT_SEC", "25")
os.environ.setdefault("ZILLOW_SCROLL_STEPS", "8")
os.environ.setdefault("ZILLOW_MAX_VISIBLE", "20")
os.environ.setdefault("ZILLOW_REQUIRE_MANUAL_CAPTCHA", "1")
os.environ.setdefault("ZILLOW_DEBUG_HEADERS", "1")   # will hit httpbin once and save HTML/PNG
os.environ.setdefault("ZILLOW_APPLY_HOME_TYPE", "1")
os.environ.setdefault("ZILLOW_APPLY_LOT_SIZE", "0")
os.environ.setdefault("ZILLOW_ACCEPT_LANG", "en-US,en;q=0.9")

def main() -> int:
    debug_dir = Path("debug"); debug_dir.mkdir(exist_ok=True)
    gecko_log = debug_dir / "geckodriver_zillow.log"
    print("\n[smoke] geckodriver log →", gecko_log.resolve())

    crit = Criteria(state="Indiana")
    ad = ZillowSeleniumAdapter()
    ad.scroll_steps = int(os.environ.get("ZILLOW_SCROLL_STEPS", "8"))
    ad.max_visible  = int(os.environ.get("ZILLOW_MAX_VISIBLE",  "20"))

    print("[smoke] starting adapter.search() …")
    n = 0
    for i, lst in enumerate(ad.search(crit), 1):
        n += 1
        print(f"[row {i:03d}] acres={lst.acres!r} price={lst.price!r} ppa={lst.price_per_acre!r} url={lst.url}")
        if i >= ad.max_visible:
            break
    print(f"[smoke] done. yielded={n}")
    print(f"[smoke] check: {gecko_log.resolve()}")
    (debug_dir / "smoke_ran.ok").write_text("ok", encoding="utf-8")
    return 0

if __name__ == "__main__":
    sys.exit(main())
