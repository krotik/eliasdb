FROM alpine:latest

ARG cluster_id=""

RUN echo "Building cluster member ${cluster_id}"

COPY ./eliasdb /eliasdb
COPY ./eliasdb.config.json /data/eliasdb.config.json
COPY ./cluster.config.json.${cluster_id} /data/cluster.config.json
COPY ./cluster.stateinfo.${cluster_id} /data/cluster.stateinfo

WORKDIR /data

CMD ["../eliasdb", "server"]
