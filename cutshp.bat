@echo off
REM Set GeoStack environment
set SCRIPTS=\\fs1\software\scripts
call %SCRIPTS%\set_geostack_environment64.bat
python %SCRIPTS%\cutshp.py %*