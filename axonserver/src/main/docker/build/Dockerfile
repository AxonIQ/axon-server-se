FROM busybox as source

# The standard distroless images have _no_ suppoprting tools, so we use a two-stage build
# to create the home directory for Axon Server and its volumes.
RUN mkdir -p /axonserver/config /axonserver/data /axonserver/events /axonserver/plugins

FROM __BASE_IMG__

COPY --from=source /axonserver /axonserver
COPY axonserver.jar axonserver.properties *.txt /axonserver/

USER root
WORKDIR /axonserver

VOLUME [ "/axonserver/config", "/axonserver/data", "/axonserver/events", "/axonserver/plugins"  ]
EXPOSE 8024/tcp 8124/tcp

ENTRYPOINT [ "java", "-jar", "./axonserver.jar" ]
