FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

RUN apt-get update && apt-get install -y \
    confluent-kafka=0.9.0.0-1 \
    java-8u20-oracle

ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20/"

COPY config.properties /server.properties

CMD echo "Kafka starting" && /usr/bin/kafka-server-start /server.properties
