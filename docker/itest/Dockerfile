FROM ubuntu:16.04
MAINTAINER Team Data Streams Core <data-streams-core@yelp.com>

ARG KAFKA_VERSION
# We need to install Java and Kafka in order to use Kafka CLI. The Kafka server
# will never run in this container; the Kafka server will run in the "kafka"
# container.

# Install Java.
RUN apt-get update && \
    apt-get install -y \
    software-properties-common \
    openjdk-8-jdk

# Install Kafka.
RUN apt-get update && apt-get install -y \
    unzip \
    wget \
    curl \
    jq \
    coreutils

ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64/"
ENV PATH "$JAVA_HOME/bin:$PATH"
ENV SCALA_VERSION="2.11"
ENV KAFKA_HOME /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}
COPY download-kafka.sh /tmp/download-kafka.sh
RUN chmod 755 /tmp/download-kafka.sh
RUN /tmp/download-kafka.sh && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV PATH="$PATH:$KAFKA_HOME/bin"

# Install Python
RUN apt-get update \
  && add-apt-repository -y ppa:deadsnakes/ppa \
  && apt-get update \
  && apt-get install -y \
    build-essential \
    libffi-dev \
    libssl-dev \
    python2.7 \
    python2.7-dev \
    python3.4 \
    python3.4-dev \
    python3.5 \
    python3.5-dev \
    python3.6 \
    python3.6-dev \
    python-pip \
    python-pkg-resources \
    python-setuptools

RUN pip install tox tox-travis

COPY run_tests.sh /scripts/run_tests.sh
RUN chmod 755 /scripts/run_tests.sh

WORKDIR /work
