FROM openjdk:8-jre
EXPOSE 8024 8124 8224

ENV LANG en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8 
ENV FC_LANG en-US

RUN apt-get update
RUN apt-get install dos2unix

ARG JAR_FILE
COPY ${JAR_FILE} app.jar

ADD maven/axoniq-cli.jar cli.jar
ADD axonhub.properties .
#ADD axoniq.license .
ADD startup.sh .
ADD register.sh .
RUN dos2unix *.properties *.sh
RUN chmod +x *.sh
ENTRYPOINT ["/startup.sh"]
