#!/bin/bash
if [ `hostname -s` = "axon-server-0" -o `hostname -s` = "axon-server" ]; then
	echo "Waiting for node to come up" 
	HEALTH_CHECK_RETURN=1
	while [ $HEALTH_CHECK_RETURN -ne 0 ]; do
		wget --quiet --spider http://localhost:8024/v1/public
		HEALTH_CHECK_RETURN=$?
		sleep 5s
	done
	echo "Adding default user and app"
	
	wget -O - http://localhost:8024/v1/applications/admin
	if [ $? -ne 0 ]; then
		java -jar cli.jar register-application -S http://localhost:8024 -a admin -r ADMIN -T token-for-my-test-cluster
		java -jar cli.jar register-user -S http://localhost:8024 -u admin -p hello -r ADMIN 
	fi
fi
