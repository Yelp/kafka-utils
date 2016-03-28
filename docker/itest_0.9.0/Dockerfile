FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

RUN apt-get update

# We need to install Java and Kafka in order to use Kafka CLI. The Kakfa server
# will never run in this container; the Kafka server will run in the "kafka"
# container.
RUN apt-get install -y java-8u20-oracle confluent-kafka=0.9.0.0-1
ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20/"
ENV PATH="$PATH:$JAVA_HOME/bin"

RUN apt-get install -y python python2.7
RUN apt-get install -y python-pkg-resources python-pip python-setuptools
RUN pip install tox

ADD run_tests.sh /scripts/run_tests.sh
RUN chmod 755 /scripts/run_tests.sh

WORKDIR /work
