FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20/"
ENV PATH="$PATH:$JAVA_HOME/bin"

# We need to install Java and Kafka in order to use Kafka CLI. The Kakfa server
# will never run in this container; the Kafka server will run in the "kafka"
# container.
RUN apt-get update && apt-get install -y \
    confluent-kafka=0.9.0.0-1 \
    java-8u20-oracle \
    python \
    python2.7 \
    python-pip \
    python-pkg-resources \
    python-setuptools

RUN pip install tox

COPY run_tests.sh /scripts/run_tests.sh
RUN chmod 755 /scripts/run_tests.sh

WORKDIR /work
