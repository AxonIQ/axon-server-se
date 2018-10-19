# AxonServer Metrics integration with Graphite

This module is packaging the required jars to use spring-actuator graphite integration as
a single jar. 

To activate the module, put the JAR file axonhub-metrics-graphite-4.x-metrics.jar in $AXONSERVER_HOME/exts directory and set the following
properties in axonserver.properties:

management.metrics.export.graphite.enabled=true
management.metrics.export.graphite.host=locahost
management.metrics.export.graphite.port=2004

When the AxonServer starts it outputs a log message:
i.a.a.m.PrometheusMetricsServer - Using Graphite integration

AxonServer adds tags axonserver/servername to all metrics.


Additional properties that can be configured:

| management.metrics.export.graphite.duration-units | java.util.concurrent.TimeUnit | milliseconds | Base time unit used to report durations. |
| management.metrics.export.graphite.protocol | io.micrometer.graphite.GraphiteProtocol | pickled | Protocol to use while shipping data to Graphite. |
| management.metrics.export.graphite.rate-units | java.util.concurrent.TimeUnit| seconds | Base time unit used to report rates. |
| management.metrics.export.graphite.step | java.time.Duration | 1m | Step size (i.e. reporting frequency) to use. |
| management.metrics.export.graphite.tags-as-prefix | [] | String[] | For the default naming convention, turn the specified tag keys into part of the\n metric prefix. |
    


