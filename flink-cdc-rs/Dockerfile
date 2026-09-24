FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3t64 liblzma5 zlib1g libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

COPY flink-cdc-rs /usr/local/bin/flink-cdc-rs

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/flink-cdc-rs"]
