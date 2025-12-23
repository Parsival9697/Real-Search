@echo off
setlocal
cd /d "%~dp0"

rem ---------- parse args ----------
set "SKIP_SETUP=0"
if /i "%1"=="--skip-setup" set "SKIP_SETUP=1"

rem ---------- hygiene ----------
set "PYTHONIOENCODING=utf-8"
if not exist "data"  mkdir "data"
if not exist "debug" mkdir "debug"
set "BROWSER_PROFILE_JSON=data\browser_profile.json"

rem ---------- Firefox binary (robust detection) ----------
rem If you know the exact path, set FIREFOX_BIN before running this script.
if not defined FIREFOX_BIN (
  if exist "%ProgramFiles%\Mozilla Firefox\firefox.exe" (
    set "FIREFOX_BIN=%ProgramFiles%\Mozilla Firefox\firefox.exe"
  ) else if exist "%ProgramFiles(x86)%\Mozilla Firefox\firefox.exe" (
    set "FIREFOX_BIN=%ProgramFiles(x86)%\Mozilla Firefox\firefox.exe"
  ) else (
    for /f "usebackq delims=" %%F in (`where firefox.exe 2^>NUL`) do (
      set "FIREFOX_BIN=%%F"
      goto :ff_found
    )
  )
)
:ff_found
set "PATH=%ProgramFiles%\Mozilla Firefox;%ProgramFiles(x86)%\Mozilla Firefox;%PATH%"

rem ---------- Zillow adapter tuning ----------
set "ZILLOW_ALLOWED=1"
set "ZILLOW_REQUIRE_MANUAL_CAPTCHA=1"
set "ZILLOW_HEADLESS=0"
set "MOZ_HEADLESS=0"
set "ZILLOW_USE_NO_REMOTE=0"

set "ZILLOW_MAX_VISIBLE=36"
set "ZILLOW_WAIT_SEC=25"
set "ZILLOW_PAUSE_BASE=1.4"
set "ZILLOW_SCROLL_STEPS=10"
set "ZILLOW_APPLY_HOME_TYPE=0"
set "ZILLOW_APPLY_LOT_SIZE=0"
set "ZILLOW_BETWEEN_COUNTIES_SEC=30"
set "ZILLOW_ACCEPT_LANG=en-US,en;q=0.9"
set "ZILLOW_WIN_W=1366"
set "ZILLOW_WIN_H=864"

rem Optional: unified client headers (UA) override
rem set "FF_UA=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0"

rem Debug headers probe inside adapter (saves a page dump/screenshot in debug/)
set "ZILLOW_DEBUG_HEADERS=0"

rem ---------- Firefox sign-in knobs ----------
rem If you don't need Firefox account sign-in anymore, set this to 0 and the setup gate will be skipped.
set "FF_ENSURE_SIGNIN=0"
set "FF_VERIFY_SYNC_ON_START=0"
set "FF_FAIL_IF_NOT_SIGNED_IN=0"
set "FF_SYNC_WAIT_SEC=300"
set "FF_SYNC_DETECT_GRACE=6"
set "FF_USE_NO_REMOTE=0"
set "FF_HEADLESS=0"

rem ---------- optional global toggles ----------
rem Import diagnostics for the package’s optional modules
set "RS_IMPORT_DEBUG=0"
set "RS_IMPORT_STRICT=0"
rem To prove CAPTCHA logic isn’t blocking your flow:
rem set "RS_DISABLE_CAPTCHA=1"

rem ---------- logging for drivers ----------
set "SELENIUM_MANAGER_LOG=1"
rem geckodriver logs are written to debug\ by Python code (FFService log_output).

rem ---------- venv ----------
if not exist ".venv\Scripts\activate.bat" (
  echo [error] Virtualenv not found at .venv\Scripts\activate.bat
  goto :eof
)
call ".venv\Scripts\activate.bat" || (
  echo [error] Could not activate virtualenv. Ensure .venv exists.
  goto :eof
)

echo [env] Headless flags: ZILLOW_HEADLESS=%ZILLOW_HEADLESS%  MOZ_HEADLESS=%MOZ_HEADLESS%  FF_VERIFY_SYNC_ON_START=%FF_VERIFY_SYNC_ON_START%
echo [env] Profile JSON: %BROWSER_PROFILE_JSON%
if defined FIREFOX_BIN (
  echo [env] Firefox bin  : %FIREFOX_BIN%
) else (
  echo [env] Firefox bin  : (not set; relying on PATH or system default)
)
echo [env] no-remote flags: ZILLOW_USE_NO_REMOTE=%ZILLOW_USE_NO_REMOTE%  FF_USE_NO_REMOTE=%FF_USE_NO_REMOTE%
echo [env] Sign-in required: FF_ENSURE_SIGNIN=%FF_ENSURE_SIGNIN%

rem If explicitly told to skip setup, jump to UI
if "%SKIP_SETUP%"=="1" goto StartUI

rem ---------- first-run setup gate (only if sign-in is required) ----------
if "%FF_ENSURE_SIGNIN%"=="0" goto StartUI

if not exist "%BROWSER_PROFILE_JSON%" goto RunSetup
findstr /c:"\"sync_ok\": true" "%BROWSER_PROFILE_JSON%" >nul
if errorlevel 1 goto RunSetup
goto StartUI

:RunSetup
echo.
echo [setup] Firefox sign-in required. A Firefox window will open.
echo [setup] Please sign in on the Settings page. We will continue when detected.
echo.
python -m crawler_service.first_run_setup --max-wait %FF_SYNC_WAIT_SEC%
if errorlevel 1 (
  echo [setup] First-run setup did not complete. See debug\geckodriver_first_run.log
  goto :eof
)
echo [setup] Setup complete. Launching UI...

:StartUI
echo.
echo [ui] Starting Streamlit UI...
python -m streamlit run "ui_app.py"
endlocal
