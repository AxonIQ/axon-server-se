# AxonServer Metrics integration with Prometheus

This module is packaging the required jars to use spring-actuator prometheus integration as
a single jar. 

To activate the module, put the JAR file axonhub-metrics-prometheus-4.x-metrics.jar in $AXONSERVER_HOME/exts directory and set the following
properties in axonserver.properties:

management.endpoint.prometheus.enabled=true
management.metrics.export.prometheus.enabled=true
management.endpoints.web.exposure.include=metrics,info,health,loggers,prometheus

When the AxonServer starts it outputs a log message:
i.a.a.m.PrometheusMetricsServer - Using PrometheusMetrics integration

Prometheus polls information from axonserver at the following endpoint:
http://localhost:8024/actuator/prometheus



  


