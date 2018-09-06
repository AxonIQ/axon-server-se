FROM ${gcloud.registry}/${gcloud.project.name}/axonhub:${project.version}
EXPOSE 8024 8124 8224

RUN apt-get update
RUN apt-get install dos2unix

ADD axoniq.pem .
ADD axoniq.license .
ADD axoniq.crt .
ADD axoniq.default.crt .
ADD axonhub.properties .
ADD startup.sh .
ADD register.sh .
ADD register_app_user.sh .

RUN dos2unix *.properties *.sh
RUN chmod +x *.sh *.jar
ENTRYPOINT ["/startup.sh"]
