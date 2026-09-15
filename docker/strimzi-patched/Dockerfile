# The Strimzi image ships Kafka as plain jars under /opt/kafka/libs and carries no source,
# so a patched build replaces two files and changes nothing else. Keep the base digest in the
# tag: a silent base bump would ship an unpatched jar pair under a patched name.
ARG STRIMZI_IMAGE=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0
FROM ${STRIMZI_IMAGE}

ARG KAFKA_VERSION=4.3.0

# No USER line on purpose: the base already runs as its own uid, and COPY runs as root at
# build time regardless. Adding one here would silently change the runtime uid.
COPY jars/kafka-clients-${KAFKA_VERSION}.jar   /opt/kafka/libs/kafka-clients-${KAFKA_VERSION}.jar
COPY jars/connect-mirror-${KAFKA_VERSION}.jar  /opt/kafka/libs/connect-mirror-${KAFKA_VERSION}.jar
