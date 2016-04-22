FROM ubuntu:14.04
MAINTAINER Team Distributed Systems <team-dist-sys@yelp.com>

RUN apt-get update && apt-get -y install zookeeper

CMD /usr/share/zookeeper/bin/zkServer.sh start-foreground
