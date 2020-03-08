#!/bin/sh

# Build the collector component

docker build --tag data-mining/collector .

# Run an interactive shell on the build image with:
# docker run -it data-mining/collector sh
