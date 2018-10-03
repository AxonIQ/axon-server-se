#!/bin/bash

export AXONSERVER_HOME=/
cd ${AXONSERVER_HOME}

if [ `hostname -s` = "axonserver-enterprise-0" -o `hostname -s` = "axonserver-enterprise" ]; then
	echo "First node in cluster - no registration"
else 
	echo "Waiting for node to come up" 
	HEALTH_CHECK_RETURN=1
	while [ $HEALTH_CHECK_RETURN -ne 0 ]; do
		wget --quiet --spider http://localhost:8024/actuator/health
		HEALTH_CHECK_RETURN=$?
		sleep 5s
	done
	echo "Registering this node with node 0" 
	java -jar cli.jar register-node -h axonserver-enterprise-0.`hostname -d`
fi
