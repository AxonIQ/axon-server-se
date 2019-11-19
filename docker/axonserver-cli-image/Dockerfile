# base image
FROM openjdk:8-jre

COPY axonserver-cli.jar axonserver-cli.jar
# Add wait-for-it
COPY wait-for-it.sh wait-for-it.sh

RUN chmod +x axonserver-cli.jar
RUN chmod +x wait-for-it.sh

ENTRYPOINT [ "/bin/bash", "-c" ]
