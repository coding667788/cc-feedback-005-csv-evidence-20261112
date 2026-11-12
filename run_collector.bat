@echo off
REM Local Windows entry point. Requires native Python 3.12 on PATH.
setlocal
if "%~1"=="" (
  echo Usage: run_collector.bat collect^|verify^|show^|batches^|doctor [args...]
  echo Example: run_collector.bat collect --config config.sample.json --manifest data\manifest.json
  exit /b 2
)
python -m csv_collector %*
exit /b %ERRORLEVEL%
