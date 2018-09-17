#!/bin/bash

export AXONSERVER_HOME=/opt/axonserver
cd ${AXONSERVER_HOME}

DOMAIN=`hostname -d`
if [[ "$DOMAIN" = "" ]]; then
  echo "No domain"
else
  echo >> ${AXONSERVER_HOME}/axonserver.properties
  echo "axoniq.axonserver.domain=$DOMAIN" >> ${AXONSERVER_HOME}/axonserver.properties
fi

java -Djava.security.egd=file:/dev/./urandom -Xmx512m -jar ${AXONSERVER_HOME}/app.jar
