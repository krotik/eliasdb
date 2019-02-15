#!/bin/sh
cd "$(dirname "$0")"

if ! [ -d "run" ]; then
  mkdir -p run
  cd run
  ../../../eliasdb server -import ../tutorial_data.zip
else
  cd run
  ../../../eliasdb server
fi
