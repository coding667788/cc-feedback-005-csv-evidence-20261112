@echo off
REM Generate the 200k-row known-answer datasets and manifest.
setlocal
python -m generator.generate_data --out data
exit /b %ERRORLEVEL%
