FROM docker-dev.yelpcorp.com/trusty_yelp
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

RUN apt-get update && apt-get -y install java-8u20-oracle confluent-kafka=0.9.0.0-1
ENV JAVA_HOME="/usr/lib/jvm/java-8-oracle-1.8.0.20/"

ADD config.properties /server.properties

CMD echo "Kafka starting" && /usr/bin/kafka-server-start /server.properties
