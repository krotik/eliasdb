#!/bin/sh
cd "$(dirname "$0")"
export ROOT_PATH=`pwd`

# Build the collector component

cp ../../../../eliasdb .
docker build --build-arg cluster_id=1 --tag data-mining/eliasdb1 .
docker build --build-arg cluster_id=2 --tag data-mining/eliasdb2 .
docker build --build-arg cluster_id=3 --tag data-mining/eliasdb3 .

# Run an interactive shell on the build image with:
# docker run -it data-mining/eliasdb sh
