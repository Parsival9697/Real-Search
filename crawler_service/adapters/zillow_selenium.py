# crawler_service/adapters/zillow_selenium.py
from __future__ import annotations
import os, json, time, random, sys, shutil
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Set

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.firefox.service import Service as FFService

from .base import SourceAdapter
from ..models import Criteria, Listing
from ..utils import parse_price, parse_acres, price_per_acre, squish_spaces
from ..captcha_helper import (
    captcha_present as helper_captcha_present,
    wait_for_captcha_clear,
)
from .zillow_nav import ZillowNavigator

# --- optional binary discovery from firefox_launcher.py -------------
try:
    # resolve_firefox_bin returns a str|None and caches to data/runtime_cache.json
    from ..firefox_launcher import resolve_firefox_bin  # type: ignore
except Exception:
    resolve_firefox_bin = None  # type: ignore

# --- EC.any_of fallback (older selenium) ---------------------------
try:
    from selenium.webdriver.support.expected_conditions import any_of as EC_ANY_OF  # type: ignore
except Exception:  # pragma: no cover
    def EC_ANY_OF(*conds):
        def _predicate(driver):
            for c in conds:
                try:
                    if c(driver):
                        return True
                except Exception:
                    pass
            return False
        return _predicate
# ------------------------------------------------------------------


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() not in {"0", "false", "no", "off", ""}


# ---------- unified client headers / privacy prefs ----------
def _apply_client_headers_prefs(opts: webdriver.FirefoxOptions) -> None:
    """
    Conservative 'normal client' prefs.

    - Only override UA / language if env vars are provided.
    - Avoid heavy-handed privacy / tracking tweaks by default.
    - Do NOT touch dom.webdriver.enabled (that is itself a fingerprint).
    """
    ua_env = (os.environ.get("FF_UA") or os.environ.get("ZILLOW_UA") or "").strip()
    lang_env = os.environ.get("ZILLOW_ACCEPT_LANG", "").strip()

    try:
        # UA / language overrides are opt-in
        if lang_env:
            opts.set_preference("intl.accept_languages", lang_env)
        if ua_env:
            opts.set_preference("general.useragent.override", ua_env)

        # Optional: referrer / cookies / etc can be relaxed via env toggles
        if _env_bool("ZILLOW_LOOSE_REFERRER", False):
            opts.set_preference("network.http.sendRefererHeader", 2)
            opts.set_preference("network.http.referer.trimmingPolicy", 0)
            opts.set_preference("network.http.referer.XOriginPolicy", 0)
            opts.set_preference("network.http.referer.XOriginTrimmingPolicy", 0)

        if _env_bool("ZILLOW_ALLOW_ALL_COOKIES", False):
            opts.set_preference("network.cookie.cookieBehavior", 0)

        if _env_bool("ZILLOW_BLOCK_GEO", False):
            opts.set_preference("permissions.default.geo", 2)
            opts.set_preference("geo.enabled", False)

        if _env_bool("ZILLOW_BLOCK_NOTIFICATIONS", False):
            opts.set_preference("permissions.default.desktop-notification", 2)
            opts.set_preference("dom.push.enabled", False)

        # Intentionally do NOT set dom.webdriver.enabled here.
    except Exception:
        # All header/pref tweaks are best-effort.
        pass

# ---------- profile resolution (no hard dependency on JSON) ----------
def _user_appdata_root() -> Path:
    if os.name == "nt":
        base = (os.environ.get("LOCALAPPDATA")
                or os.environ.get("APPDATA")
                or str(Path.home()))
        return Path(base) / "RealSearch"
    elif sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "RealSearch"
    else:
        return Path.home() / ".local" / "share" / "realsearch"

def _fallback_profile_dir() -> Path:
    root = _user_appdata_root()
    root.mkdir(parents=True, exist_ok=True)
    prof = root / "ff_profile"
    prof.mkdir(parents=True, exist_ok=True)
    return prof

def _load_profile_json_if_present(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        p = (d.get("firefox_profile_path") or "").strip()
        if p and Path(p).exists():
            return p
    except Exception:
        return None
    return None


class ZillowSeleniumAdapter(SourceAdapter):
    name = "zillow_selenium_ui"

    def _captcha_env_dump(self, drv, tag: str) -> None:
        try:
            ua = drv.execute_script("return navigator.userAgent || ''") or ""
            langs = drv.execute_script("return navigator.languages || []") or []
            width = drv.execute_script("return window.innerWidth || 0") or 0
            height = drv.execute_script("return window.innerHeight || 0") or 0
            ck_len = drv.execute_script("return (document.cookie||'').length") or 0
        except Exception:
            ua, langs, width, height, ck_len = "", [], 0, 0, 0
        self._log(f"[captcha:{tag}] UA='{ua[:140]}' langs={langs} vp={width}x{height} cookie_len={ck_len}")
        try:
            ts = self._ts()
            img = self.debug_dir / f"zillow_captcha_{tag}_{ts}.png"
            html = self.debug_dir / f"zillow_captcha_{tag}_{ts}.html"
            drv.save_screenshot(str(img))
            html.write_text(drv.page_source or "", encoding="utf-8", errors="ignore")
            self._log(f"[captcha:{tag}] saved debug: {img.name}, {html.name}")
        except Exception:
            pass

    def __init__(self) -> None:
        # core toggles
        self.enabled = _env_bool("ZILLOW_ALLOWED", False)
        self.max_visible = int(os.environ.get("ZILLOW_MAX_VISIBLE", "120"))
        self.wait_sec = int(os.environ.get("ZILLOW_WAIT_SEC", "15"))
        self.pause_base = float(os.environ.get("ZILLOW_PAUSE_BASE", "0.6"))
        self.scroll_steps = int(os.environ.get("ZILLOW_SCROLL_STEPS", "24"))
        self.apply_home_type = _env_bool("ZILLOW_APPLY_HOME_TYPE", False)
        self.apply_lot_size = _env_bool("ZILLOW_APPLY_LOT_SIZE", False)
        self.between_counties = float(os.environ.get("ZILLOW_BETWEEN_COUNTIES_SEC", "8"))

        # pacing & typing
        self.initial_sleep = float(os.environ.get("ZILLOW_INITIAL_SLEEP", "1.0"))
        self.type_delay_min = float(os.environ.get("ZILLOW_TYPE_DELAY_MIN", "0.06"))
        self.type_delay_max = float(os.environ.get("ZILLOW_TYPE_DELAY_MAX", "0.16"))
        self.enter_pause = float(os.environ.get("ZILLOW_ENTER_PAUSE", "1.2"))
        self.after_nav_pause = float(os.environ.get("ZILLOW_AFTER_NAV_PAUSE", "2.0"))
        self.click_suggestion = _env_bool("ZILLOW_CLICK_SUGGESTION", True)
        self.post_captcha_grace_sec = float(os.environ.get("ZILLOW_POST_CAPTCHA_GRACE_SEC", "4.0"))

        # CAPTCHA first so headless default can depend on it
        self.require_manual_captcha = _env_bool("ZILLOW_REQUIRE_MANUAL_CAPTCHA", True)
        self.captcha_max_wait = int(os.environ.get("CAPTCHA_MAX_WAIT_SEC", "300"))

        # IMPORTANT:
        # - When manual solve is required, DO NOT auto-refresh while under challenge.
        # - Auto refreshes on the PX wall are a great way to get bounced to a hard block.
        if self.require_manual_captcha:
            # default 0; you can override with env if you *really* want
            self.captcha_refresh = int(os.environ.get("CAPTCHA_REFRESH_AFTER_SEC", "0"))
        else:
            self.captcha_refresh = int(os.environ.get("CAPTCHA_REFRESH_AFTER_SEC", "90"))

        # Extra grace so slow machines can fully load any CAPTCHA wall
        self.captcha_settle_sec = int(os.environ.get("ZILLOW_CAPTCHA_SETTLE_SEC", "10"))

        # Visible browser when manual solve is required
        default_headless = False if self.require_manual_captcha else True
        self.headless = _env_bool("ZILLOW_HEADLESS", default_headless)
        if self.require_manual_captcha and self.headless:
            self.headless = False  # hard override for manual CAPTCHA

        # Do NOT touch MOZ_HEADLESS here; _make_driver will own that.
        # We only clear obviously conflicting flags (except MOZ_HEADLESS) so
        # callers can't silently force headless against our policy.
        if not self.headless:
            for k in ("FF_HEADLESS", "ZILLOW_HEADLESS"):
                os.environ.pop(k, None)

        # Firefox sign-in preflight — default DISABLED now
        self.ensure_signin = _env_bool("FF_ENSURE_SIGNIN", False)
        self.sync_wait_total = int(os.environ.get("FF_SYNC_WAIT_SEC", "300"))
        self._did_signin_check = False

        # quick verify & fail-fast guard — default DISABLED now
        self.verify_sync_on_start = _env_bool("FF_VERIFY_SYNC_ON_START", False)
        self.fail_if_not_signed_in = _env_bool("FF_FAIL_IF_NOT_SIGNED_IN", False)
        self._did_quick_verify = False

        # debug
        self.debug = _env_bool("ZILLOW_DEBUG", False)
        self.keep_open_on_error = _env_bool("ZILLOW_KEEP_OPEN_ON_ERROR", False)
        self.debug_dir = Path("debug")
        self.debug_dir.mkdir(exist_ok=True)

        # Single place that knows how to “wake up” the Zillow homepage and submit a phrase
        # If your ZillowNavigator has the new dials (max_attempts, reload_between_attempts), pass them here.
        try:
            self.nav = ZillowNavigator(
                log=self._log,
                wait_sec=self.wait_sec,
                after_nav_pause=self.after_nav_pause,
                click_suggestion=self.click_suggestion,
                captcha_present=self._captcha_present,
                wait_for_captcha_clear=self._wait_for_captcha_clear,
                max_attempts=3,
                reload_between_attempts=False,
            )
        except TypeError:
            # Fallback if your ZillowNavigator version doesn't support the new kwargs yet
            self.nav = ZillowNavigator(
                log=self._log,
                wait_sec=self.wait_sec,
                after_nav_pause=self.after_nav_pause,
                click_suggestion=self.click_suggestion,
                captcha_present=self._captcha_present,
                wait_for_captcha_clear=self._wait_for_captcha_clear,
            )

        # profile is OPTIONAL now
        self.use_profile = _env_bool("ZILLOW_USE_PROFILE", True)
        self.profile_json_path = Path(os.environ.get("BROWSER_PROFILE_JSON", "data/browser_profile.json"))
        self.profile_path: Optional[str] = None
        if self.use_profile:
            # priority: explicit env → valid JSON → fallback per-user dir
            env_profile = (os.environ.get("ZILLOW_FIREFOX_PROFILE") or os.environ.get("RS_PROFILE_DIR") or "").strip()
            if env_profile and Path(env_profile).exists():
                self.profile_path = str(Path(env_profile).resolve())
            else:
                p = _load_profile_json_if_present(self.profile_json_path)
                if p:
                    self.profile_path = p
                else:
                    # persistent but local per-user dir (no JSON dependency)
                    self.profile_path = str(_fallback_profile_dir().resolve())

        self._log(
            f"init: headless={self.headless}, require_manual_captcha={self.require_manual_captcha}, "
            f"ensure_signin={self.ensure_signin}, profile={'(none)' if not self.use_profile else self.profile_path}, "
            f"MOZ_HEADLESS={os.environ.get('MOZ_HEADLESS')}"
        )

    # ---------- logging & helpers ----------
    def _log(self, msg: str) -> None:
        print(f"[zillow] {msg}", flush=True)

    def _ts(self) -> str:
        from datetime import datetime as _dt
        return _dt.now().strftime("%Y%m%d_%H%M%S")

    def _save_debug(self, drv, tag: str) -> None:
        if not self.debug:
            return
        try:
            ts = self._ts()
            img = self.debug_dir / f"zillow_{tag}_{ts}.png"
            html = self.debug_dir / f"zillow_{tag}_{ts}.html"
            drv.save_screenshot(str(img))
            html.write_text(drv.page_source or "", encoding="utf-8", errors="ignore")
            self._log(f"saved debug: {img.name}, {html.name}")
        except Exception as e:
            self._log(f"debug save failed: {e}")

    def _pause(self, mult: float = 1.0) -> None:
        time.sleep(self.pause_base * mult + random.uniform(0.05, 0.25))

    # ---------- CAPTCHA wrappers ----------
    # ---------- CAPTCHA wrappers ----------

    def _captcha_present(self, drv) -> bool:
        # Thin wrapper so we can log/disable in one place.
        try:
            return helper_captcha_present(drv)
        except Exception:
            return False

    def _wait_for_captcha_clear(self, drv, where: str, max_wait_sec: Optional[float] = None) -> bool:
        """
        When require_manual_captcha=True:
          - Do NOT poke the DOM trying to be smart.
          - Let the human solve the challenge.
          - Resume ONLY after explicit confirmation in the terminal.
        Otherwise:
          - Delegate to captcha_helper.wait_for_captcha_clear for automated waiting.
        """
        # Manual mode: be dumb and safe.
        if self.require_manual_captcha:
            self._log(f"[captcha:{where}] Challenge detected.")
            self._log(
                f"[captcha:{where}] Please solve the CAPTCHA in Firefox, "
                f"then press ENTER here to continue …"
            )
            try:
                # Block until user confirms; no DOM interaction here.
                input()
            except Exception:
                # If stdin isn't available, fall back to a simple delay.
                self._log(f"[captcha:{where}] stdin unavailable; sleeping 15s instead.")
                time.sleep(15.0)

            # One conservative check after user-confirm:
            try:
                if self._captcha_present(drv):
                    self._log(f"[captcha:{where}] Still seeing CAPTCHA after confirm; aborting this area.")
                    return False
            except Exception:
                pass

            self._log(f"[captcha:{where}] Proceeding after manual confirmation.")
            return True

        # Non-manual mode: use the helper's smarter wait logic.
        try:
            return wait_for_captcha_clear(
                drv,
                where=where,
                max=int(max_wait_sec or self.captcha_max_wait),
                refresh_after=self.captcha_refresh,
                log=self._log,
            )
        except TypeError:
            # In case of signature mismatch with helper, call minimal version.
            return wait_for_captcha_clear(drv, where=where)  # type: ignore
        except Exception as e:
            self._log(f"[captcha:{where}] wait_for_captcha_clear failed: {e}")
            return False

    # ---------- (disabled-by-default) quick verify ----------
    def _quick_verify_signed_in_once(self) -> None:
        if self._did_quick_verify or not self.verify_sync_on_start:
            return
        self._did_quick_verify = True

        if self.require_manual_captcha:
            self._log("Skipping headless Sync quick-verify (manual CAPTCHA enabled).")
            return

        opts = webdriver.FirefoxOptions()
        opts.add_argument("--headless")
        try:
            opts.set_preference("network.cookie.cookieBehavior", 0)
            if self.use_profile and self.profile_path:
                opts.profile = self.profile_path
        except Exception:
            pass

        # FIREFOX_BIN → env, else resolve via firefox_launcher if available
        bin_hint = (os.environ.get("FIREFOX_BIN") or "").strip()
        if not bin_hint and resolve_firefox_bin:
            try:
                r = resolve_firefox_bin()
                bin_hint = r or ""
            except Exception:
                bin_hint = ""
        if bin_hint and Path(bin_hint).exists():
            try:
                opts.binary_location = bin_hint
                self._log(f"using FIREFOX_BIN for quick-verify: {bin_hint}")
            except Exception:
                pass

        drv = webdriver.Firefox(options=opts)
        try:
            drv.set_page_load_timeout(30)
            drv.get("about:preferences#sync")
            WebDriverWait(drv, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            body = (drv.page_source or "").lower()
            signed = ("disconnect" in body) or ("manage account" in body) or ("sign out" in body)
            if not signed and self.fail_if_not_signed_in:
                raise RuntimeError(
                    "Firefox profile not signed in (FF_FAIL_IF_NOT_SIGNED_IN=1). "
                    "Disable sign-in checks or sign in manually."
                )
            if not signed:
                self._log("Sync not detected; continuing (verification disabled by default).")
        finally:
            try:
                drv.quit()
            except Exception:
                pass

    # ---------- (disabled-by-default) sign-in preflight helpers ----------
    def _is_signed_in_sync_page(self, drv) -> bool:
        try:
            body = (drv.page_source or "").lower()
        except Exception:
            body = ""
        needles = ["manage account", "disconnect", "signed in as", "sign out", "account connected"]
        if any(n in body for n in needles):
            return True
        try:
            signin_ctas = drv.find_elements(
                By.XPATH,
                "//button[contains(., 'Sign in') or contains(., 'Sign In') or contains(., 'Turn on Sync')]"
            )
            if signin_ctas:
                return False
        except Exception:
            pass
        return False

    def _click_sign_in_if_present(self, drv) -> bool:
        selectors = [
            (By.XPATH, "//button[contains(., 'Sign in') or contains(., 'Sign In') or contains(., 'Turn on Sync')]"),
            (By.CSS_SELECTOR, "button[data-l10n-id*='fxa']"),
            (By.XPATH, "//a[contains(., 'Sign in') or contains(., 'Sign In')]"),
        ]
        for by, sel in selectors:
            try:
                el = WebDriverWait(drv, 5).until(EC.element_to_be_clickable((by, sel)))
                if el and el.is_displayed():
                    drv.execute_script("arguments[0].click();", el)
                    return True
            except Exception:
                continue
        return False

    def _switch_to_newest_window(self, drv) -> None:
        try:
            handles = drv.window_handles
            if handles:
                drv.switch_to.window(handles[-1])
        except Exception:
            pass

    def _url_lc(self, drv) -> str:
        try:
            return (drv.current_url or "").lower()
        except Exception:
            return ""

    def _is_accounts(self, url_lc: str) -> bool:
        return ("accounts.firefox.com" in url_lc) or ("about:accounts" in url_lc)

    def _is_prefs(self, url_lc: str) -> bool:
        return url_lc.startswith("about:preferences")

    def _saw_accounts_iframe(self, drv) -> bool:
        try:
            frames = drv.find_elements(By.TAG_NAME, "iframe")
            for fr in frames:
                try:
                    src = (fr.get_attribute("src") or "").lower()
                    if "accounts.firefox.com" in src:
                        return True
                except Exception:
                    continue
        except Exception:
            pass
        return False

    def _ensure_firefox_signed_in(self, drv) -> None:
        if self._did_signin_check or not self.ensure_signin:
            return
        try:
            drv.maximize_window()
        except Exception:
            pass

        self._log("Opening Firefox Sync preferences to verify login …")
        drv.get("about:preferences#sync")
        try:
            WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            pass

        clicked = self._click_sign_in_if_present(drv)
        if clicked:
            self._log("Clicked 'Sign in' — waiting for accounts.firefox.com …")
            time.sleep(1.0)

        start = time.time()
        total_cap = self.sync_wait_total
        grace_after_leave = int(os.environ.get("FF_SYNC_DETECT_GRACE", "8"))
        last_left_accounts_at = None
        saw_accounts = False
        reopened_prefs_once = False

        while time.time() - start < total_cap:
            time.sleep(1.0)
            self._switch_to_newest_window(drv)

            url = self._url_lc(drv)
            on_accounts = self._is_accounts(url) or self._saw_accounts_iframe(drv)
            on_prefs = self._is_prefs(url)

            if on_accounts:
                saw_accounts = True
                last_left_accounts_at = None
                continue

            if saw_accounts and last_left_accounts_at is None:
                last_left_accounts_at = time.time()

            if on_prefs:
                if self._is_signed_in_sync_page(drv):
                    self._log("Firefox account appears signed in — proceeding to Zillow.")
                    self._did_signin_check = True
                    return

                if saw_accounts and last_left_accounts_at and (
                        time.time() - last_left_accounts_at >= grace_after_leave):
                    self._log("Finished FxA flow; could not positively detect UI strings, proceeding anyway.")
                    self._did_signin_check = True
                    return

                if self._click_sign_in_if_present(drv):
                    continue
                continue

            if saw_accounts and not reopened_prefs_once:
                self._log("Returning to Sync preferences to re-check sign-in state …")
                try:
                    drv.get("about:preferences#sync")
                    reopened_prefs_once = True
                    continue
                except Exception:
                    pass
            continue

        if self.fail_if_not_signed_in:
            raise RuntimeError("Timeout waiting for Firefox Sign-In (FF_FAIL_IF_NOT_SIGNED_IN=1).")
        self._log("Did not detect a signed-in Sync state before timeout — proceeding anyway.")
        self._did_signin_check = True

    # ---------- driver ----------
    def _make_driver(self) -> webdriver.Firefox:
        """
        Launch Firefox with strong preference for a persistent profile via options.profile.
        Falls back to legacy FirefoxProfile, then a vanilla temp profile.
        Always logs to debug/geckodriver_zillow.log.
        """
        base_opts = webdriver.FirefoxOptions()

        # Visible if we need manual CAPTCHA or sign-in; otherwise honor self.headless.
        headless = bool(self.headless and not self.ensure_signin)

        # Own MOZ_HEADLESS here (and only here)
        if headless:
            os.environ["MOZ_HEADLESS"] = "1"
            base_opts.add_argument("--headless")
        else:
            os.environ.pop("MOZ_HEADLESS", None)

        _apply_client_headers_prefs(base_opts)

        # Binary: env → firefox_launcher.resolve_firefox_bin → PATH
        bin_hint = (os.environ.get("FIREFOX_BIN") or "").strip()
        if not bin_hint and resolve_firefox_bin:
            try:
                r = resolve_firefox_bin()
                bin_hint = r or ""
            except Exception:
                bin_hint = ""
        if bin_hint and Path(bin_hint).exists():
            try:
                base_opts.binary_location = bin_hint
                self._log(f"using FIREFOX_BIN: {bin_hint}")
            except Exception:
                pass
        elif bin_hint:
            self._log(f"FIREFOX_BIN provided but not found: {bin_hint}")

        ts = self._ts()
        gecko_log = (self.debug_dir / f"geckodriver_zillow_{ts}.log").resolve()
        service = FFService(log_output=str(gecko_log))
        self._log(f"geckodriver log at: {gecko_log}")  # keeps the path visible in your console

        use_no_remote = _env_bool("ZILLOW_USE_NO_REMOTE", False)

        def _clone_opts() -> webdriver.FirefoxOptions:
            o = webdriver.FirefoxOptions()
            # Copy arguments but strip any accidental --headless
            for a in getattr(base_opts, "arguments", []):
                if str(a).strip().lower() != "--headless":
                    o.add_argument(a)
            _apply_client_headers_prefs(o)
            # carry over binary if set
            try:
                if getattr(base_opts, "binary_location", None):
                    o.binary_location = base_opts.binary_location
            except Exception:
                pass
            if use_no_remote:
                o.add_argument("-no-remote")
                o.add_argument("-new-instance")
            return o

        # If using a persistent profile, clear stale lock defensively
        if self.use_profile and self.profile_path:
            try:
                lock_file = Path(self.profile_path) / "parent.lock"
                if lock_file.exists():
                    lock_file.unlink()
            except Exception:
                pass

        # --------- ATTEMPT A: options.profile (preferred) ----------
        try:
            o1 = _clone_opts()
            if self.use_profile and self.profile_path:
                o1.profile = self.profile_path  # the reliable way
                self._log(
                    f"launch attempt A: options.profile = {self.profile_path} (headless={headless}, no-remote={use_no_remote})"
                )
            else:
                self._log(f"launch attempt A: options.profile not set (use_profile={self.use_profile})")

            drv = webdriver.Firefox(options=o1, service=service)
            self._post_window_setup(drv)
            self._force_bring_to_front(drv)
            self._log("Firefox driver is up (mode A: options.profile).")
            self._log(f"geckodriver log at: {gecko_log}")
            return drv
        except Exception as eA:
            self._log(f"options.profile launch failed: {eA}")

        # --------- ATTEMPT B: legacy FirefoxProfile ----------
        try:
            from selenium.webdriver import FirefoxProfile  # type: ignore
            o2 = _clone_opts()
            self._log("launch attempt B: legacy FirefoxProfile")
            if self.use_profile and self.profile_path:
                fp = FirefoxProfile(self.profile_path)  # type: ignore
                drv = webdriver.Firefox(options=o2, firefox_profile=fp, service=service)
            else:
                drv = webdriver.Firefox(options=o2, service=service)
            self._post_window_setup(drv)
            self._force_bring_to_front(drv)
            self._log("Firefox driver is up (mode B: legacy FirefoxProfile).")
            self._log(f"geckodriver log at: {gecko_log}")
            return drv
        except Exception as eB:
            self._log(f"legacy FirefoxProfile launch failed: {eB}")

        # --------- ATTEMPT C: vanilla temp profile (last resort) ----------
        try:
            o3 = _clone_opts()
            self._log("launch attempt C: vanilla temp profile (no persistent profile)")
            drv = webdriver.Firefox(options=o3, service=service)
            self._post_window_setup(drv)
            self._force_bring_to_front(drv)
            self._log("Firefox driver is up (mode C: vanilla).")
            self._log(f"geckodriver log at: {gecko_log}")
            return drv
        except Exception as eC:
            self._log(f"vanilla launch failed: {eC}")
            raise

    # --- begin stable window helpers ---
    def _post_window_setup(self, drv) -> None:
        """Idempotent: set a reasonable size/position once; no maximize/fullscreen."""
        if getattr(self, "_window_prepped", False):
            try:
                drv.set_page_load_timeout(self.wait_sec + 10)
            except Exception:
                pass
            return

        try:
            w = int(os.environ.get("ZILLOW_WIN_W", "1360"))
            h = int(os.environ.get("ZILLOW_WIN_H", "860"))
        except Exception:
            w, h = 1360, 860

        try:
            drv.set_window_rect(x=80, y=60, width=max(1100, w), height=max(780, h))
        except Exception:
            pass
        try:
            drv.set_page_load_timeout(self.wait_sec + 10)
        except Exception:
            pass

        self._window_prepped = True

    def _maybe_show_window(self, drv) -> None:
        """Only make the window visible AFTER CAPTCHA is gone, and only once."""
        if getattr(self, "_window_shown", False):
            return
        self._post_window_setup(drv)
        try:
            drv.execute_script("window.focus && window.focus();")
        except Exception:
            pass
        self._window_shown = True

    def _force_bring_to_front(self, drv) -> None:
        """Legacy callers may still invoke this. Keep it gentle and idempotent."""
        self._maybe_show_window(drv)
    # --- end stable window helpers ---

    # ---------- UI actions ----------
    def _wait_body(self, drv) -> None:
        WebDriverWait(drv, self.wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    def _maybe_accept_cookies(self, drv) -> None:
        for by, sel in [
            (By.XPATH, "//button[contains(., 'Accept') or contains(., 'accept')]"),
            (By.CSS_SELECTOR, "button[aria-label*='Accept']"),
        ]:
            try:
                el = drv.find_element(by, sel)
                if el.is_displayed():
                    drv.execute_script("arguments[0].click();", el)
                    self._pause(0.8)
                    return
            except Exception:
                pass

    def _ensure_for_sale(self, drv) -> None:
        for by, sel in [
            (By.XPATH, "//a[contains(., 'For Sale') or contains(., 'For sale')]"),
            (By.XPATH, "//button[contains(., 'For Sale') or contains(., 'For sale')]"),
            (By.CSS_SELECTOR, "a[data-testid*='for-sale']"),
        ]:
            try:
                el = drv.find_element(by, sel)
                if el.is_displayed():
                    drv.execute_script("arguments[0].click();", el)
                    self._pause(1.0)
                    return
            except Exception:
                continue

    def _apply_home_type_lots_land(self, drv) -> None:
        if not self.apply_home_type:
            return
        try:
            opened = False
            for by, sel in [
                (By.XPATH, "//button[contains(., 'Home type')]"),
                (By.XPATH, "//button[contains(., 'Type')]"),
                (By.CSS_SELECTOR, "button[aria-label*='Home type']"),
            ]:
                try:
                    el = drv.find_element(by, sel)
                    if el.is_displayed():
                        drv.execute_script("arguments[0].click();", el)
                        self._pause(0.8)
                        opened = True
                        break
                except Exception:
                    continue
            if not opened:
                return

            for by, sel in [
                (By.XPATH, "//label[contains(., 'Lots/Land') or contains(., 'Lot/Land')]/input"),
                (By.XPATH, "//input[@type='checkbox' and contains(@aria-label, 'Lot')]"),
            ]:
                try:
                    chk = drv.find_element(by, sel)
                    if not chk.is_selected():
                        drv.execute_script("arguments[0].click();", chk)
                        self._pause(0.4)
                    break
                except Exception:
                    continue

            for by, sel in [
                (By.XPATH, "//button[contains(., 'Apply')]"),
                (By.XPATH, "//button[contains(., 'Done')]"),
            ]:
                try:
                    el = drv.find_element(by, sel)
                    if el.is_displayed():
                        drv.execute_script("arguments[0].click();", el)
                        break
                except Exception:
                    continue
            self._pause(1.0)
        except Exception:
            pass

    def _apply_lot_size(self, drv, min_acres: Optional[float], max_acres: Optional[float]) -> None:
        if not self.apply_lot_size:
            return
        try:
            opener = None
            for by, sel in [
                (By.XPATH, "//button[contains(., 'More')]"),
                (By.CSS_SELECTOR, "button[aria-label*='More']"),
                (By.XPATH, "//button[contains(., 'Lot size')]"),
                (By.XPATH, "//button[contains(., 'Lot Size')]"),
            ]:
                try:
                    el = drv.find_element(by, sel)
                    if el.is_displayed():
                        opener = el; break
                except Exception:
                    continue
            if not opener:
                return

            drv.execute_script("arguments[0].click();", opener)
            self._pause(0.8)

            def _set(cands, val: Optional[float]) -> None:
                if val is None or val <= 0:
                    return
                for by, sel in cands:
                    try:
                        box = drv.find_element(by, sel)
                        if box.is_displayed():
                            box.clear(); self._pause(0.2)
                            box.send_keys(str(int(round(val))))
                            self._pause(0.2)
                            return
                    except Exception:
                        continue

            _set([
                (By.CSS_SELECTOR, "input[aria-label*='Lot Size Min']"),
                (By.CSS_SELECTOR, "input[placeholder*='Min']"),
            ], min_acres)

            _set([
                (By.CSS_SELECTOR, "input[aria-label*='Lot Size Max']"),
                (By.CSS_SELECTOR, "input[placeholder*='Max']"),
            ], max_acres)

            for by, sel in [
                (By.XPATH, "//button[contains(., 'Apply')]"),
                (By.XPATH, "//button[contains(., 'Done')]"),
                (By.CSS_SELECTOR, "button[data-testid*='apply']"),
            ]:
                try:
                    el = drv.find_element(by, sel)
                    if el.is_displayed():
                        drv.execute_script("arguments[0].click();", el)
                        break
                except Exception:
                    continue
            self._pause(1.0)
        except Exception:
            pass

    # ---------- typing & results ----------
    def _type_slowly(self, el, text: str) -> None:
        for ch in text:
            el.send_keys(ch)
            time.sleep(random.uniform(self.type_delay_min, self.type_delay_max))

    def _pick_typeahead(self, drv, phrase: str) -> bool:
        try:
            WebDriverWait(drv, max(3, int(self.wait_sec / 5))).until(
                EC_ANY_OF(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "[data-testid='typeahead-item']")
                    ),
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "li[role='option']")
                    ),
                )
            )
        except Exception:
            return False

        items = drv.find_elements(
            By.CSS_SELECTOR, "[data-testid='typeahead-item'], li[role='option']"
        )
        if not items:
            items = drv.find_elements(
                By.XPATH, "//*[@data-testid='typeahead-item'] | //li[@role='option']"
            )
        if not items:
            return False

        phrase_low = phrase.lower().replace(",", " ")
        parts = [p for p in phrase_low.split() if p]

        best = None
        for el in items:
            try:
                t = (el.text or "").strip()
            except Exception:
                continue
            tl = t.lower()
            if "county" in phrase_low and "county" in tl and all(p in tl for p in parts):
                best = el
                break
            if best is None and all(p in tl for p in parts):
                best = el

        if best is None:
            best = items[0]

        try:
            drv.execute_script("arguments[0].scrollIntoView({block:'center'});", best)
            time.sleep(0.2)
            drv.execute_script("arguments[0].click();", best)
            return True
        except Exception:
            return False

    def _find_search_input(self, drv):
        for by, sel in [
            (By.CSS_SELECTOR, "input[data-testid='search-box-input']"),
            (By.CSS_SELECTOR, "input[aria-label*='City']"),
            (By.CSS_SELECTOR, "input[role='combobox']"),
            (By.CSS_SELECTOR, "input[type='text']"),
            (By.XPATH, "//input"),
        ]:
            try:
                el = WebDriverWait(drv, self.wait_sec).until(EC.element_to_be_clickable((by, sel)))
                if el and el.is_displayed():
                    return el
            except Exception:
                continue
        return None

    def _collect_card_anchors(self, drv) -> List:
        anchors = drv.find_elements(By.CSS_SELECTOR, "[data-testid='search-result-card'] a[href*='/homedetails/']")
        if not anchors:
            anchors = drv.find_elements(By.XPATH, "//a[contains(@href, '/homedetails/')]")
        return anchors

    def _gentle_scroll(self, drv) -> None:
        drv.execute_script("window.scrollBy(0, Math.floor(window.innerHeight * 0.85));")
        self._pause(0.9)

    def _read_card(self, anchor) -> Tuple[str, Optional[float], Optional[float], str]:
        href = (anchor.get_attribute("href") or "").split("?")[0]
        container = anchor
        try:
            container = anchor.find_element(
                By.XPATH, "./ancestor::article|./ancestor::div[@data-testid='search-result-card']"
            )
        except Exception:
            pass
        txt = ""
        try:
            txt = container.text or ""
        except Exception:
            try:
                txt = anchor.text or ""
            except Exception:
                txt = ""
        t = squish_spaces(txt or "")
        price = parse_price(t)
        acres = parse_acres(t)
        return href, price, acres, t

    def _looks_like_land(self, text: str) -> bool:
        t = (text or "").lower()
        if "acre" in t:
            return True
        for kw in ("lot", "vacant", "land", "parcel", "tract", "farm", "ranch", "pasture", "timber"):
            if kw in t:
                return True
        return False

    # ---------- public ----------
    def search(self, criteria: Criteria) -> Iterable[Listing]:
        if not self.enabled:
            self._log("adapter disabled (set ZILLOW_ALLOWED=1).")
            return

        if not self.require_manual_captcha:
            self._quick_verify_signed_in_once()
        else:
            self._log("Skipping headless quick-verify because manual CAPTCHA is enabled.")

        counties: List[str] = []
        c = criteria.county_normalized
        if c:
            counties = [c]

        if not counties:
            yield from self._run_one_area(criteria.state, None, criteria)
        else:
            for i, cty in enumerate(counties):
                yield from self._run_one_area(criteria.state, cty, criteria)
                if i < len(counties) - 1:
                    time.sleep(self.between_counties + random.uniform(0.2, 1.2))

    def _run_one_area(self, state: str, county: Optional[str], criteria: Criteria) -> Iterable[Listing]:
        drv = None
        try:
            self._log("creating Firefox driver …")
            drv = self._make_driver()
            self._force_bring_to_front(drv)
            self._log("driver created — proceeding to Zillow homepage")

            # Optional: probe headers once if requested
            if _env_bool("ZILLOW_DEBUG_HEADERS", False):
                try:
                    self._log("debug headers probe → https://httpbin.org/headers")
                    drv.get("https://httpbin.org/headers")
                    self._wait_body(drv)
                    self._pause(0.6)
                    self._save_debug(drv, "headers_probe")
                except Exception as e:
                    self._log(f"headers probe failed: {e}")

            # (disabled-by-default) ensure Firefox is signed in once per run
            self._ensure_firefox_signed_in(drv)

            # ------------------------------------------------------------------
            # Zillow homepage — give CAPTCHA time to appear BEFORE doing anything
            # ------------------------------------------------------------------
            drv.get("https://www.zillow.com/")

            # Wait for base DOM so we're not checking against a half-loaded shell.
            try:
                self._wait_body(drv)
            except Exception:
                pass

            # On slower machines / networks, the PX "Press & Hold" widget can take
            # several seconds to load. If we start typing immediately, Zillow may
            # treat it as automation under the challenge and loop "Please try again".
            settle = max(0, int(self.captcha_settle_sec))
            if settle:
                self._log(f"[captcha] settle wait {settle}s to let any challenge load before searching …")
                time.sleep(settle)

            # After the settle window, check for a visible CAPTCHA and, if present,
            # wait for *you* to solve it WITHOUT touching the page.
            if self._captcha_present(drv):
                self._save_debug(drv, "captcha_home_seen")
                if self.require_manual_captcha:
                    self._log("CAPTCHA detected at home — waiting (no automation) for manual solve …")
                    if not self._wait_for_captcha_clear(drv, where="home"):
                        self._log("CAPTCHA did not clear in time; aborting this area.")
                        return
                    # Small grace period after it disappears so inputs re-enable.
                    time.sleep(0.8 + random.uniform(0.4, 0.9))
                    # Grace period after it disappears so PX + UI fully settle.
                    time.sleep(self.post_captcha_grace_sec + random.uniform(0.4, 1.2))
                else:
                    self._log("CAPTCHA at homepage and manual solve not enabled; skipping.")
                    return

            # Now it should be safe to bring the window forward & work with UI.
            self._force_bring_to_front(drv)
            try:
                drv.execute_script("window.focus && window.focus();")
            except Exception:
                pass

            self._maybe_accept_cookies(drv)
            time.sleep(self.initial_sleep)

            # --- Kick off the search via the navigator (single call) ---
            phrase = f"{county} County, {state}" if county else state
            self._log(f"searching phrase: {phrase}")

            if not self.nav.kickstart_search(drv, phrase):
                self._log("Could not navigate to results; skipping area.")
                return

            # Breath after successful nav
            time.sleep(self.after_nav_pause)

            # If a CAPTCHA appears AFTER navigation, handle it once and continue.
            if self._captcha_present(drv):
                self._save_debug(drv, "captcha_after_nav")
                if self.require_manual_captcha:
                    self._log("CAPTCHA appeared after navigation — waiting for manual solve …")
                    if not self._wait_for_captcha_clear(drv, where="after_nav"):
                        self._log("CAPTCHA (after_nav) did not clear; skipping area.")
                        return
                    time.sleep(0.8 + random.uniform(0.4, 0.9))
                else:
                    self._log("CAPTCHA after navigation and manual solve not enabled; skipping area.")
                    return

            # After navigator returns, give the page a breath to settle.
            time.sleep(self.after_nav_pause)

            # If a CAPTCHA popped during/after the nav, clear it and continue.
            if self._captcha_present(drv):
                self._save_debug(drv, "captcha_after_nav")
                if self.require_manual_captcha:
                    if not self._wait_for_captcha_clear(drv, where="after_nav", max_wait_sec=self.captcha_max_wait):
                        return
                    time.sleep(0.6 + random.uniform(0.2, 0.5))
                else:
                    self._log("CAPTCHA after navigation; skipping area.")
                    return

            # Apply filters and start collecting
            self._ensure_for_sale(drv)
            self._apply_home_type_lots_land(drv)
            self._apply_lot_size(drv, criteria.min_acres, criteria.max_acres)

            seen: Set[str] = set()
            yielded, stagnant = 0, 0
            for _ in range(self.scroll_steps):
                anchors = self._collect_card_anchors(drv)
                grew = False
                for a in anchors:
                    href, price, acres, card_text = self._read_card(a)
                    if not href or href in seen:
                        continue
                    seen.add(href)
                    if not self.apply_home_type and not self._looks_like_land(card_text):
                        continue

                    grew = True
                    ppa = price_per_acre(price, acres)
                    lst = Listing(
                        source=self.name, url=href, title=None,
                        price=price, acres=acres, price_per_acre=ppa,
                        extras={"state": state, "county": county or "(Any)", "on_page": True},
                    )
                    if self.keep_by_criteria(lst, criteria):
                        yield lst
                        yielded += 1
                        if yielded >= self.max_visible:
                            self._log(f"{county or state}: reached max_visible={self.max_visible}; stopping.")
                            return
                stagnant = 0 if grew else (stagnant + 1)
                if stagnant >= 3:
                    break
                self._gentle_scroll(drv)
                if self._captcha_present(drv):
                    self._save_debug(drv, "captcha_scrolling")
                    if self.require_manual_captcha:
                        if not self._wait_for_captcha_clear(drv, where="scrolling", max_wait_sec=240.0):
                            break
                    else:
                        self._log("captcha while scrolling; stopping area.")
                        break

            self._pause(1.0)

        except WebDriverException as e:
            self._log(f"webdriver error: {e}")
            if drv is not None:
                self._save_debug(drv, "webdriver_error")
        finally:
            if drv is not None:
                try:
                    if not (self.keep_open_on_error and self.debug):
                        self._log("quitting Firefox driver.")
                        drv.quit()
                except Exception:
                    pass
