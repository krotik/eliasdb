#!/bin/sh
# Query the score nodes in the main game world
../../eliasdb console -exec "get score"
# Query the conf node in the main game world
../../eliasdb console -exec "get conf"
