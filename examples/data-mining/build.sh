#!/bin/sh
cd "$(dirname "$0")"
export ROOT_PATH=`pwd`

# This build script should build the following images in the local Docker registry:
#
# data-mining/frontend
# data-mining/eliasdb1
# data-mining/eliasdb2
# data-mining/eliasdb3
# data-mining/collector

echo Building Collector
echo ==================
cd ./docker-images/collector
./build.sh
cd $ROOT_PATH

echo
echo Building Eliasdb Cluster
echo ========================
cd docker-images/eliasdb
./build.sh
cd $ROOT_PATH

echo
echo Building Frontend
echo =================
cd docker-images/frontend
./build.sh
cd $ROOT_PATH

