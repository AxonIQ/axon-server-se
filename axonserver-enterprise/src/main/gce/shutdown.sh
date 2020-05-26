#!/bin/bash

AXONSERVER_PIDFILE=/var/lib/axonserver/AxonIQ.pid
if [ ! -s ${AXONSERVER_PIDFILE} ] ; then
    echo "There is no Axon Server node active named \"${AXONSERVER_WORK}\"."
    exit 1
fi

AXONSERVER_PID=`cat ${AXONSERVER_PIDFILE} | sed s/@.*$//`

echo "Asking Axon Server to quit. (process ID ${AXONSERVER_PID})"
kill ${AXONSERVER_PID}

countDown=5
while ps -p ${AXONSERVER_PID} >/dev/null ; do
    sleep 5

    countDown=`expr ${countDown} - 1`
    if [[ "x${countDown}" = "x" -o "x${countDown}" = "x0" ]]  ; then
        break
    fi
done

if ps -p ${AXONSERVER_PID} >/dev/null ; then
    echo "Killing Axon Server forcefully (process ID ${AXONSERVER_PID})"
    kill -9 ${AXONSERVER_PID}
    rm -f ${AXONSERVER_PIDFILE}
fi

exit 0