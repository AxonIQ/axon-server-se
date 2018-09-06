#!/bin/bash

NAMESPACE=${K8S_NAMESPACE}
if [[ "$NAMESPACE" = "" ]]; then
  echo "No namespace"
else
  echo >> axonhub.properties
  echo "axoniq.axonhub.domain=axonhub.$NAMESPACE.svc.cluster.local" >> axonhub.properties
  if [[ -f axoniq.$NAMESPACE.crt ]]; then
    cp axoniq.$NAMESPACE.crt axoniq.crt
  fi
fi

/register.sh &
/register_app_user.sh &
java -Djava.security.egd=file:/dev/./urandom -Xmx512m  -jar app.jar
