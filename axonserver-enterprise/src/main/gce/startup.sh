#!/bin/bash

cd ${HOME}

if [ ! -s ${HOME}/init-done ] ; then

    HOSTNAME=$(hostname)
    echo "Initialising Axon Server home \"${HOME}\" on \"${HOSTNAME}\"."
    ${HOME}/check-link.sh --create-target ${HOME}/control /mnt/${HOSTNAME}-data/control
    ${HOME}/check-link.sh --create-target ${HOME}/log /mnt/${HOSTNAME}-data/log
    ${HOME}/check-link.sh ${HOME}/events /mnt/${HOSTNAME}-events

    if [ ! -s ${HOME}/axonserver.properties ] ; then
        echo "Creating empty properties file"
        touch ${HOME}/axonserver.properties
    fi

    echo "Init done" > ${HOME}/init-done
else
    echo "Skipping Initialisation after reboot."
fi

echo "Checking for property overrides."
if curl -s -H "Metadata-Flavor:Google" -o ${HOME}/axonserver.properties.override http://metadata.google.internal/computeMetadata/v1/instance/attributes/axonserver-properties
then
    ${HOME}/get-property-names.sh --properties ${HOME}/axonserver.properties.override | while read prop ; do
        oldValue=$(${HOME}/get-property-value.sh --properties ${HOME}/axonserver.properties ${prop} )
        value=$(${HOME}/get-property-value.sh --properties ${HOME}/axonserver.properties.override ${prop} )

        if [[ "${oldValue}" != "${value}" ]] ; then
            echo "Overriding \"${prop}\" from \"${oldValue}\" to \"${value}\"."
            ${HOME}/set-property.sh ${prop} ${value}
        else
            echo "Property \"${prop}\" is already set to \"${value}\"."
        fi
    done
else
    echo "No property overrides found."
fi

echo "Downloading license file."
curl -s -H "Metadata-Flavor:Google" -o ${HOME}/axoniq.license http://metadata.google.internal/computeMetadata/v1/instance/attributes/axoniq-license

java -jar ${HOME}/axonserver.jar >/dev/null 2>&1
