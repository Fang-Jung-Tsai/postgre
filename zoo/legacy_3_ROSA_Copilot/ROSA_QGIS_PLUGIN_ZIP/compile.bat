@echo off
call "C:\OSGeo4W\bin\o4w_env.bat"

@echo on
cd %~dp0
pyrcc5 -o resources.py resources.qrc
pause
