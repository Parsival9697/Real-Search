# crawler_service/adapters/zillow_nav.py
from __future__ import annotations

import time
import random
from typing import Callable, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Fallback for EC.any_of across selenium versions
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


class ZillowNavigator:
    """
    Minimal navigation helper for Zillow’s homepage → results flow.

    Design goals (important for avoiding "bot jail"):
      - Do NOT compete with the adapter’s CAPTCHA handling.
      - Avoid noisy DOM sweeps and reload loops.
      - Only touch the real search box, slowly and minimally.
    """

    def __init__(
        self,
        *,
        log: Callable[[str], None],
        wait_sec: int = 15,
        after_nav_pause: float = 2.0,
        click_suggestion: bool = True,
        # CAPTCHA hooks supplied by the adapter:
        captcha_present: Optional[Callable] = None,
        wait_for_captcha_clear: Optional[Callable[..., bool]] = None,
        # Robustness knobs
        max_attempts: int = 2,
        reload_between_attempts: bool = False,  # default: NO reload loops
        pause_after_reload: float = 0.8,
    ):
        self.log = log
        self.wait_sec = int(wait_sec)
        self.after_nav_pause = float(after_nav_pause)
        self.click_suggestion = bool(click_suggestion)

        self.captcha_present = captcha_present
        self.wait_for_captcha_clear = wait_for_captcha_clear

        self.max_attempts = int(max_attempts)
        self.reload_between_attempts = bool(reload_between_attempts)
        self.pause_after_reload = float(pause_after_reload)

    # ---------- public entry ----------

    def kickstart_search(self, drv, phrase: str) -> bool:
        """
        From Zillow homepage (or a plain landing view), ensure the search input
        is visible/focused, type `phrase`, and navigate to results.

        Assumes the adapter has already:
          - Loaded https://www.zillow.com/
          - Waited out any initial CAPTCHA when required.

        Returns True if we appear to land on a sensible results page.
        """

        # Small human-ish delay so any challenge / heavy JS has time to appear.
        time.sleep(1.0 + random.uniform(0.3, 0.9))

        # If the adapter gave us a captcha hook, respect it and do NOTHING else
        # until it reports clear.
        if self.captcha_present and self.captcha_present(drv):
            if self.wait_for_captcha_clear:
                self.log("CAPTCHA visible at nav start — delegating to wait_for_captcha_clear …")
                if not self.wait_for_captcha_clear(drv, where="home"):
                    return False
                time.sleep(0.6 + random.uniform(0.2, 0.5))
            else:
                # Caller said there may be captcha but gave no waiter — bail.
                self.log("CAPTCHA detected but no wait_for_captcha_clear hook; aborting search.")
                return False

        # Main attempts loop. We default to very few retries to avoid
        # drawing attention with repeated scripted submits.
        for attempt in range(1, self.max_attempts + 1):
            for attempt in range(1, self.max_attempts + 1):
                # small human-ish think time before each attempt
                time.sleep(0.4 + random.uniform(0.2, 0.8))

                # Re-check modal strictly before each attempt
                if not self._wait_modal_clear(drv, cap_seconds=180):
                    self.log("Blocking modal persisted too long; aborting attempt.")
                    return False
            # If a non-CAPTCHA blocking modal is up (rare), wait briefly.
            if not self._wait_modal_clear(drv, cap_seconds=15):
                self.log("Blocking modal did not clear quickly; aborting.")
                return False

            # If a CAPTCHA appears here, adapter owns it; we step aside.
            if self.captcha_present and self.captcha_present(drv):
                if self.wait_for_captcha_clear:
                    self.log(f"CAPTCHA detected before attempt {attempt} — delegating to adapter …")
                    if not self.wait_for_captcha_clear(drv, where="home"):
                        return False
                    time.sleep(0.6 + random.uniform(0.2, 0.5))
                else:
                    return False

            # Locate or wake the search input.
            self._open_home_search_box(drv)
            box = self._find_search_input(drv)
            if not box:
                self._save_debug_if(drv, f"no_search_input_attempt_{attempt}")
                self.log("Search input not found; backing off a bit.")
                if self.reload_between_attempts:
                    try:
                        drv.get("https://www.zillow.com/")
                    except Exception:
                        pass
                    time.sleep(self.pause_after_reload)
                else:
                    time.sleep(0.8 + random.uniform(0.1, 0.3))
                continue

            # Type phrase slowly and submit.
            self._type_phrase(drv, box, phrase)

            used_suggestion = False
            if self.click_suggestion and self._pick_typeahead(drv, phrase):
                used_suggestion = True
            if not used_suggestion:
                try:
                    box.send_keys(Keys.ENTER)
                except Exception:
                    pass

            # Wait for something that looks like a results context.
            ok = self._wait_results_context(drv)
            time.sleep(self.after_nav_pause)

            # If a CAPTCHA pops DURING or AFTER nav, let adapter drive it.
            if self.captcha_present and self.captcha_present(drv):
                self._save_debug_if(drv, f"captcha_after_nav_attempt_{attempt}")
                if self.wait_for_captcha_clear:
                    self.log("CAPTCHA appeared after navigation — delegating to adapter …")
                    if not self.wait_for_captcha_clear(drv, where="after_nav"):
                        return False
                    time.sleep(1.0 + random.uniform(0.3, 0.7))
                else:
                    return False
                # After human solve, loop back and try again gently.
                continue

            # Sanity-check that we’re on a plausible results page.
            if ok and self._results_looks_like(drv, phrase):
                return True

            # Not convincing; one more gentle retry at most.
            self._save_debug_if(drv, f"no_results_match_attempt_{attempt}")
            self.log(f"Results didn’t look scoped to “{phrase}” (attempt {attempt}); backing off.")
            if self.reload_between_attempts:
                try:
                    drv.get("https://www.zillow.com/")
                except Exception:
                    pass
                time.sleep(self.pause_after_reload)
            else:
                time.sleep(1.0 + random.uniform(0.2, 0.6))

        # All attempts failed.
        self._save_debug_if(drv, "no_results_context_after_retries")
        self.log("Results page did not render or match phrase after retries.")
        return False

    # ---------- internals ----------

    def _wait_modal_clear(self, drv, *, cap_seconds: int) -> bool:
        """
        Wait briefly until no NON-CAPTCHA blocking modal is present.

        If a CAPTCHA is present (detected via the adapter hook), we bail out and
        let the adapter handle it; we return True so kickstart_search can defer
        to wait_for_captcha_clear instead of looping here.
        """
        t0 = time.time()
        while time.time() - t0 < cap_seconds:
            # If caller wired captcha_present and it's true, *do not* fight it.
            if self.captcha_present:
                try:
                    if self.captcha_present(drv):
                        return True
                except Exception:
                    pass

            if not self._blocking_modal_present(drv):
                return True

            time.sleep(0.6)
        return False

    def _blocking_modal_present(self, drv) -> bool:
        """
        Detect a generic, large blocking dialog that is NOT specifically a
        captcha we’ve delegated upwards.

        Implementation is intentionally narrow: we only look at a small number
        of likely overlay/dialog containers instead of sweeping the entire DOM.
        """
        try:
            return bool(drv.execute_script(
                """
                try {
                    const candidates = Array.from(
                      document.querySelectorAll(
                        "[role='dialog'], div[aria-modal='true'], " +
                        "div[data-testid*='lightbox'], div[data-testid*='modal']"
                      )
                    );

                    for (const el of candidates) {
                        const style = window.getComputedStyle(el);
                        if (!style) continue;
                        const vis = style.display !== 'none' &&
                                    style.visibility !== 'hidden' &&
                                    parseFloat(style.opacity || '1') > 0.1;
                        if (!vis) continue;

                        const r = el.getBoundingClientRect();
                        const bigEnough =
                          r.width  > window.innerWidth  * 0.4 &&
                          r.height > window.innerHeight * 0.3;

                        if (!bigEnough) continue;

                        const txt = (el.innerText || "").toLowerCase();
                        if (!txt) continue;

                        // Generic "blocking" hints; avoid explicit captcha phrasing
                        // so that proper captcha handling is done by the adapter.
                        if (txt.includes("before we continue") ||
                            txt.includes("access to this page has been") ||
                            txt.includes("update your browser")) {
                            return true;
                        }
                    }
                    return false;
                } catch (e) {
                    return false;
                }
                """
            ))
        except Exception:
            return False

    def _open_home_search_box(self, drv) -> None:
        """
        Focus the main search input gently.
        - No container mashing.
        - Minimal selectors.
        - No action if a blocking modal is present.
        """
        if self._blocking_modal_present(drv):
            return

        try:
            # tiny think-time before interacting
            time.sleep(0.3 + random.uniform(0.1, 0.3))

            drv.execute_script("""
                try {
                    const cand =
                        document.querySelector("input[data-testid='search-box-input']") ||
                        document.querySelector("input[role='searchbox']") ||
                        document.querySelector("input[role='combobox']") ||
                        document.querySelector("header input[type='search']") ||
                        document.querySelector("main input[type='search']");
                    if (cand) {
                        cand.scrollIntoView({block:'center'});
                        cand.focus();
                    }
                } catch (e) {}
            """)
            time.sleep(0.15)

            # As a fallback, click just the real input (not wrappers)
            for by, sel in [
                (By.CSS_SELECTOR, "input[data-testid='search-box-input']"),
                (By.CSS_SELECTOR, "input[role='searchbox']"),
                (By.CSS_SELECTOR, "input[role='combobox']"),
            ]:
                try:
                    el = drv.find_element(by, sel)
                    if el.is_displayed():
                        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        time.sleep(0.1)
                        drv.execute_script("arguments[0].click();", el)
                        break
                except Exception:
                    continue

            time.sleep(0.15 + random.uniform(0.05, 0.15))
        except Exception:
            pass

    def _find_search_input(self, drv):
        if self._blocking_modal_present(drv):
            return None

        selectors = [
            (By.CSS_SELECTOR, "input[data-testid='search-box-input']"),
            (By.CSS_SELECTOR, "input[role='combobox']"),
            (By.CSS_SELECTOR, "input[role='searchbox']"),
            (By.CSS_SELECTOR, "form[action*='/homes/'] input[type='search']"),
            (By.CSS_SELECTOR, "form[action*='/homes/'] input[type='text']"),
            (By.CSS_SELECTOR, "header input[type='search'], main input[type='search']"),
            (By.CSS_SELECTOR, "header input[type='text'],   main input[type='text']"),
        ]

        deadline = time.time() + max(6, int(self.wait_sec / 2))
        while time.time() < deadline:
            if self._blocking_modal_present(drv):
                return None

            for by, sel in selectors:
                try:
                    el = WebDriverWait(drv, 2).until(
                        EC.element_to_be_clickable((by, sel))
                    )
                    if el and el.is_displayed():
                        try:
                            drv.execute_script(
                                "arguments[0].scrollIntoView({block:'center'});"
                                "arguments[0].focus && arguments[0].focus();",
                                el,
                            )
                        except Exception:
                            pass
                        return el
                except Exception:
                    continue

            # Lightweight JS probe as last resort.
            try:
                el = drv.execute_script(
                    """
                    return document.querySelector("input[data-testid='search-box-input']") ||
                           document.querySelector("form[action*='/homes/'] input[type='search']") ||
                           document.querySelector("form[action*='/homes/'] input[type='text']") ||
                           document.querySelector("input[role='combobox']") ||
                           document.querySelector("input[role='searchbox']") ||
                           document.querySelector("header input[type='search'], main input[type='search']") ||
                           document.querySelector("header input[type='text'],   main input[type='text']");
                    """
                )
                if el:
                    return el
            except Exception:
                pass

            time.sleep(0.25)

        return None

    def _type_phrase(self, drv, el, phrase: str) -> None:
        # Gentle clear only if needed
        try:
            has_val = bool(drv.execute_script("return !!arguments[0].value;", el))
        except Exception:
            has_val = False

        if has_val:
            try:
                el.send_keys(Keys.CONTROL, "a")
                time.sleep(0.08 + random.uniform(0.02, 0.06))
                el.send_keys(Keys.DELETE)
                time.sleep(0.08 + random.uniform(0.02, 0.06))
            except Exception:
                pass

        # brief 'thinking' pause before typing
        time.sleep(0.35 + random.uniform(0.15, 0.45))

        # Type with small jitter
        for i, ch in enumerate(phrase):
            try:
                el.send_keys(ch)
            except Exception:
                pass
            time.sleep(0.06 + random.uniform(0.015, 0.07))
            if i and i % 4 == 0:
                time.sleep(0.05 + random.uniform(0.02, 0.08))

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
                By.XPATH,
                "//*[@data-testid='typeahead-item'] | //li[@role='option']",
            )
        if not items:
            return False

        phrase_low = phrase.lower().replace(",", " ")
        parts = [p for p in phrase_low.split() if p]

        best = None
        for el in items:
            try:
                t = (el.text or "").strip().lower()
            except Exception:
                continue
            if "county" in phrase_low and "county" in t and all(p in t for p in parts):
                best = el
                break
            if best is None and all(p in t for p in parts):
                best = el

        if best is None:
            best = items[0]

        try:
            drv.execute_script(
                "arguments[0].scrollIntoView({block:'center'});", best
            )
            time.sleep(0.12)
            drv.execute_script("arguments[0].click();", best)
            return True
        except Exception:
            return False

    def _wait_results_context(self, drv) -> bool:
        try:
            WebDriverWait(drv, max(8, self.wait_sec)).until(
                EC_ANY_OF(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "[data-testid='search-result-card']")
                    ),
                    EC.presence_of_element_located(
                        (By.ID, "search-page-list-container")
                    ),
                    EC.url_contains("/homes/"),
                    EC.url_contains("for_sale"),
                )
            )
            return True
        except Exception:
            self._save_debug_if(drv, "no_results_context_after_submit")
            self.log("results page did not render in time.")
            return False

    def _results_looks_like(self, drv, phrase: str) -> bool:
        """
        Heuristic: URL + (title OR cards) match the search phrase.
        """
        target = phrase.lower().replace(",", " ")
        parts = [p for p in target.split() if p]

        # URL check
        try:
            url = (drv.current_url or "").lower()
        except Exception:
            url = ""
        url_ok = ("/homes/" in url) or ("for_sale" in url)

        # Title check
        title_ok = False
        try:
            title = (drv.title or "").lower()
            if title:
                hit = sum(1 for p in parts if p in title)
                title_ok = hit >= max(1, len(parts) // 2)
        except Exception:
            pass

        # Cards present
        has_cards = False
        try:
            cards = drv.find_elements(
                By.CSS_SELECTOR, "[data-testid='search-result-card']"
            )
            has_cards = len(cards) > 0
        except Exception:
            pass

        return bool(url_ok and (title_ok or has_cards))

    def _save_debug_if(self, drv, tag: str) -> None:
        """
        Placeholder: the adapter owns screenshots & HTML dumps.
        Intentionally no-op here to avoid duplicate I/O.
        """
        try:
            _ = hasattr(drv, "save_screenshot")
        except Exception:
            pass
