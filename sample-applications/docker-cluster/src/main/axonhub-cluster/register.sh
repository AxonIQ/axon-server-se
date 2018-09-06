#!/bin/bash
if [ `hostname -s` = "axonhub-0" -o `hostname -s` = "axonhub" ]; then
	echo "First node in cluster - no registration"
else 
	echo "Waiting for node to come up" 
	HEALTH_CHECK_RETURN=1
	while [ $HEALTH_CHECK_RETURN -ne 0 ]; do
		wget --quiet --spider http://localhost:8024
		HEALTH_CHECK_RETURN=$?
		sleep 5s
	done

	curl -s -o /dev/null -w "%{http_code}" -f http://localhost:8024/v1/cluster/axonhub-0
	if [[ $? != 0 ]]; then
	    echo "Registering this node with node 0"
	    java -jar cli.jar register-node -S http://localhost:8024 -h axonhub-0.`hostname -d` -p 8224
	fi
fi
