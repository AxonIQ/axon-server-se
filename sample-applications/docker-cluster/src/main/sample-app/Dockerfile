FROM openjdk:8-jre
EXPOSE 8080

RUN apt-get update
RUN apt-get install dos2unix

ADD maven/sample-command-processor1.jar app.jar

ADD application.properties .
ADD axoniq.crt .
ADD axoniq.default.crt .
ADD startup.sh .

RUN dos2unix *.properties *.sh
RUN chmod +x *.sh
ENTRYPOINT ["/startup.sh"]
