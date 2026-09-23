FROM ubuntu:24.10

COPY ../target/release/flink-cdc-rs /opt

WORKDIR /opt

ENTRYPOINT ["/opt/flink-cdc-rs"]