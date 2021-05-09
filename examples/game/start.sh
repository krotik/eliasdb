#!/bin/sh
cd "$(dirname "$0")"

if ! [ -d "run" ]; then
  mkdir -p run/web
  cp -fR res/eliasdb.config.json run
  cp -fR res/scripts run
  cp -fR res/frontend/*.html run/web
  cp -fR res/frontend/assets run/web
  cp -fR res/frontend/dist run/web
fi
cd run
../../../eliasdb server -ecal-console
