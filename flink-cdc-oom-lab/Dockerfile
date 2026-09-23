FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates iptables \
    && rm -rf /var/lib/apt/lists/*

COPY target/release/flink-cdc-oom-lab /opt/flink-cdc-oom-lab
COPY deploy/fault-kafka.sh /opt/fault-kafka.sh

RUN chmod 0755 /opt/flink-cdc-oom-lab /opt/fault-kafka.sh

ENTRYPOINT ["/opt/flink-cdc-oom-lab"]
