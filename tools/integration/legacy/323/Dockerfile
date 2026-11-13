# SPDX-License-Identifier: Apache-2.0
# Verified Apache archive, packaged locally on a pinned Docker Official Java 17 base.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title="Isolated Apache Kafka 3.2.3 lab"
LABEL kafka.archive.source="https://archive.apache.org/dist/kafka/3.2.3/kafka_2.13-3.2.3.tgz"
LABEL kafka.archive.sha512="4a57d69f8d3b5158ddca9c64ac366c2975ca55f4c2ef02cd3b122e127016ad1a5aa1de92e7e2f392fdc3b57aabe75d4631373d77a3a5958c28d911f4034a136f"
COPY kafka_2.13-3.2.3.tgz /tmp/kafka.tgz
RUN echo "4a57d69f8d3b5158ddca9c64ac366c2975ca55f4c2ef02cd3b122e127016ad1a5aa1de92e7e2f392fdc3b57aabe75d4631373d77a3a5958c28d911f4034a136f  /tmp/kafka.tgz" | sha512sum -c - \
    && mkdir -p /opt/kafka /opt/lab/config /var/lib/kafka/data /var/lib/zookeeper \
    && tar -xzf /tmp/kafka.tgz --strip-components=1 -C /opt/kafka \
    && rm /tmp/kafka.tgz \
    && chown -R 1000:1000 /var/lib/kafka /var/lib/zookeeper
ENV JAVA_HOME=/opt/java/openjdk
ENV HOME=/tmp
USER 1000:1000
WORKDIR /opt/kafka
