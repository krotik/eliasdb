@echo off
cd "%~p1"

if NOT EXIST run (
  mkdir run
  cd run
  ..\..\..\eliasdb server -import ..\tutorial_data.zip
) ELSE (
  cd run
  ..\..\..\eliasdb server
)
