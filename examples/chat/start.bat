@echo off
cd "%~p1"

if NOT EXIST run (
  mkdir run
  cd run
  mkdir web
  xcopy /e ..\res\chat\* web
  copy ..\res\eliasdb.config.json .
  copy ..\res\access.db .
  cd ..  
)
cd run
..\..\..\eliasdb server
