#!/bin/sh
cd "$(dirname "$0")"

if ! [ -d "run" ]; then
  mkdir -p run/web
  mkdir -p run/scripts
  cp -fR res/chat/* run/web
  cp -fR res/scripts/* run/scripts
  cp -fR res/eliasdb.config.json run
  cp -fR res/access.db run
fi
cd run
../../../eliasdb server
