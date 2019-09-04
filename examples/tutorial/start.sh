#!/bin/sh
cd "$(dirname "$0")"

if ! [ -d "run" ]; then
  mkdir -p run
  cd run
  mkdir web
  cp -fR ../res/graphiql web
  ../../../eliasdb server -import ../res/tutorial_data.zip
else
  cd run
  ../../../eliasdb server
fi
