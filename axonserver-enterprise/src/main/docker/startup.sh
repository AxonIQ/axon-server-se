#!/bin/bash

DOMAIN=`hostname -d`
if [[ "$DOMAIN" = "" ]]; then
  echo "No domain"
else
  echo >> axonserver.properties
  echo "axoniq.axonserver.domain=$DOMAIN" >> axonserver.properties
fi

/register.sh &
/register_app_user.sh &
java -Djava.security.egd=file:/dev/./urandom -Xmx512m -jar app.jar
