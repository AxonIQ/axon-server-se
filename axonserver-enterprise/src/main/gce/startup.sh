#!/bin/bash

cd ${HOME}

if [ ! -s ${HOME}/init-done ] ; then

    HOSTNAME=$(hostname)
    echo "Initialising Axon Server home \"${HOME}\" on \"${HOSTNAME}\"."
    ${HOME}/check-link.sh --create-target ${HOME}/control /mnt/${HOSTNAME}-data/control
    ${HOME}/check-link.sh --create-target ${HOME}/log /mnt/${HOSTNAME}-data/log
    ${HOME}/check-link.sh ${HOME}/events /mnt/${HOSTNAME}-events

    echo "Initialising Axon Server properties."
    touch ${HOME}/axonserver.properties
    ${HOME}/set-property.sh spring.profiles.active axoniq-cloud-support
    ${HOME}/set-property.sh logging.file ${HOME}/axonserver-enterprise.log
    ${HOME}/set-property.sh axoniq.axonserver.cluster.enabled true
    ${HOME}/set-property.sh axoniq.axonserver.hostname ${HOSTNAME}
    ${HOME}/set-property.sh axoniq.axonserver.internal-hostname ${HOSTNAME}
    ${HOME}/set-property.sh axoniq.axonserver.domain cloud.axoniq.net
    ${HOME}/set-property.sh axoniq.axonserver.internal-domain cloud.axoniq.net
    ${HOME}/set-property.sh axoniq.axonserver.event.storage ./events
    ${HOME}/set-property.sh axoniq.axonserver.snapshot.storage ./events
    ${HOME}/set-property.sh axoniq.axonserver.controldb-path ./control
    ${HOME}/set-property.sh axoniq.axonserver.replication.log-storage ./log

    if [ ! -s axoniq.license ] ; then
        curl -s -H "Metadata-Flavor:Google" -o ${HOME}/axoniq.license http://metadata.google.internal/computeMetadata/v1/instance/attributes/axoniq-license
    fi

    echo "Init done" > ${HOME}/init-done
else
    echo "Skipping Initialisation after reboot."
fi

nohup java -jar ${HOME}/axonserver.jar &
