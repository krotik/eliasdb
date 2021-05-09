@echo off
cd "%~p1"

if NOT EXIST run (
  mkdir run
  cd run
  mkdir web
  mkdir scripts
  xcopy /e ..\res\chat\* web
  xcopy /e ..\res\scripts\* scrips
  copy ..\res\eliasdb.config.json .
  copy ..\res\access.db .
  cd ..  
)
cd run
..\..\..\eliasdb server
