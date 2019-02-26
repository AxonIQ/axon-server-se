FROM openjdk:8-jre
EXPOSE 8024 8124 8224

ENV LANG en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8 
ENV FC_LANG en-US

RUN apt-get update && apt-get install dos2unix

ENV AXONSERVER_HOME /opt/axonserver
RUN mkdir -p ${AXONSERVER_HOME}

ARG JAR_FILE
COPY ${JAR_FILE} ${AXONSERVER_HOME}/app.jar

ADD maven/axoniq-cli.jar ${AXONSERVER_HOME}/cli.jar
ADD axonserver.properties ${AXONSERVER_HOME}/axonserver.properties
ADD startup.sh ${AXONSERVER_HOME}/startup.sh
RUN dos2unix ${AXONSERVER_HOME}/*.properties ${AXONSERVER_HOME}/*.sh
RUN chmod +x ${AXONSERVER_HOME}/*.sh

ENTRYPOINT ["/opt/axonserver/startup.sh"]
