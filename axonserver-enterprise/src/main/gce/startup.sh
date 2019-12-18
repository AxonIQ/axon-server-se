#!/bin/bash

cd ${HOME}

if [ ! -s init-done ] ; then
    echo "" >> application.yml

    echo "Init done" > init-done
fi

nohup java -jar axonserver.jar &
