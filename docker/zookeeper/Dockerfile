FROM ubuntu:18.04
MAINTAINER Team Data Streams Core <data-streams-core@yelp.com>
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -y install zookeeper

CMD /usr/share/zookeeper/bin/zkServer.sh start-foreground
