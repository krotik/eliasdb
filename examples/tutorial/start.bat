@echo off
cd "%~p1"

if NOT EXIST run (
  mkdir run
  cd run
  mkdir web
  xcopy /e ..\res\graphiql web
  ..\..\..\eliasdb server -import ..\tutorial_data.zip
) ELSE (
  cd run
  ..\..\..\eliasdb server
)
