@echo off
REM Full automated test suite (stdlib unittest only).
setlocal
python -m unittest discover -s tests -v
exit /b %ERRORLEVEL%
