FROM ubuntu:16.04
MAINTAINER Team Data Streams Core <data-streams-core@yelp.com>

ARG KAFKA_VERSION

# Install Kafka.
RUN apt-get update && apt-get install -y \
    unzip \
    wget \
    curl \
    jq \
    coreutils \
    openjdk-8-jdk

ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64/"
ENV PATH "$PATH:$JAVA_HOME/bin"
ENV SCALA_VERSION="2.11"
ENV KAFKA_HOME /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}
COPY download-kafka.sh /tmp/download-kafka.sh
RUN chmod 755 /tmp/download-kafka.sh
RUN /tmp/download-kafka.sh && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV PATH="$PATH:$KAFKA_HOME/bin"

COPY config.properties /server.properties

CMD echo "Kafka starting" && kafka-server-start.sh /server.properties
