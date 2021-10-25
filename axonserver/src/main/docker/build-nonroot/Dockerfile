FROM busybox as source
RUN addgroup -S -g 1001 axonserver \
    && adduser -S -u 1001 -G axonserver -h /axonserver -D axonserver \
    && mkdir -p /axonserver/config /axonserver/data /axonserver/events /axonserver/plugins \
    && chown -R axonserver:axonserver /axonserver

FROM __BASE_IMG__

COPY --from=source /etc/passwd /etc/group /etc/
COPY --from=source --chown=axonserver /axonserver /axonserver

COPY --chown=axonserver axonserver.jar axonserver.properties *.txt /axonserver/

USER axonserver
WORKDIR /axonserver

VOLUME [ "/axonserver/config", "/axonserver/data", "/axonserver/events", "/axonserver/plugins" ]
EXPOSE 8024/tcp 8124/tcp

ENTRYPOINT [ "java", "-jar", "./axonserver.jar" ]
