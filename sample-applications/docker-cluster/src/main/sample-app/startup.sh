#!/bin/bash

NAMESPACE=${K8S_NAMESPACE}
if [[ "$NAMESPACE" = "" ]]; then
  echo "No namespace"
  echo >> axonhub.properties
  echo "axoniq.axonhub.servers=axonhub.axoniq" >> application.properties
else
  echo "axoniq.axonhub.servers=axonhub-0.axonhub.$NAMESPACE.svc.cluster.local,axonhub-1.axonhub.$NAMESPACE.svc.cluster.local,axonhub-2.axonhub.$NAMESPACE.svc.cluster.local" >> application.properties

  if [[ -f axoniq.$NAMESPACE.crt ]]; then
    cp axoniq.$NAMESPACE.crt axoniq.crt
  fi
fi

java -jar app.jar	
