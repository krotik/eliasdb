#!/bin/sh

# Copy graphiql from examples

cp -fR ../../../tutorial/res/graphiql ./app

# Build the collector component

docker build --tag data-mining/frontend .

# Run container
# docker run -p 8080:80 data-mining/frontend

# Run an interactive shell on the build image with:
# docker run -it data-mining/frontend sh
