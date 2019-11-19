package io.axoniq.axonserver;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsConfiguration;
import io.opencensus.exporter.trace.jaeger.JaegerExporterConfiguration;
import io.opencensus.exporter.trace.jaeger.JaegerTraceExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import javax.annotation.PreDestroy;

/**
 * Configuration of Axon Server Enterprise gRPC metrics.
 *
 * @author Milan Savic
 * @since 4.2.1
 */
@Configuration
@ConditionalOnProperty("axoniq.axonserver.metrics.grpc.enabled")
public class AxonServerEnterpriseGrpcMetricsConfig {

    private boolean jaegerEnabled = false;

    /**
     * Registers all necessary views for metrics and tracing.
     *
     * @param metricsConfigProperties gRPC monitoring configuration properties
     * @param prometheusMeterRegistry micrometer Prometheus registry used for registering gRPC metrics
     */
    @Autowired
    public void configureGrpcMetrics(GrpcMonitoringProperties metricsConfigProperties,
                                     PrometheusMeterRegistry prometheusMeterRegistry)
            throws IOException {
        TraceConfig traceConfig = Tracing.getTraceConfig();
        traceConfig.updateActiveTraceParams(
                traceConfig.getActiveTraceParams()
                           .toBuilder()
                           .setSampler(Samplers.alwaysSample())
                           .build());
        // we are using a deprecated method here since exporters are not updated yet to work with RpcViews.registerAllGrpcViews();
        RpcViews.registerAllViews();

        if (metricsConfigProperties.iszPagedEnabled()) {
            ZPageHandlers.startHttpServerAndRegisterAll(metricsConfigProperties.getzPagesPort());
        }

        if (metricsConfigProperties.isPrometheusEnabled()) {
            PrometheusStatsCollector.createAndRegister(PrometheusStatsConfiguration.builder()
                                                                                   .setRegistry(
                                                                                           prometheusMeterRegistry
                                                                                                   .getPrometheusRegistry())
                                                                                   .build());
        }

        if (metricsConfigProperties.isJaegerEnabled()) {
            jaegerEnabled = true;
            JaegerExporterConfiguration jaegerConf =
                    JaegerExporterConfiguration.builder()
                                               .setServiceName(metricsConfigProperties.getJaegerServiceName())
                                               .setThriftEndpoint(metricsConfigProperties.getJaegerEndpoint())
                                               .build();
            JaegerTraceExporter.createAndRegister(jaegerConf);
        }
    }

    @PreDestroy
    public void clean() {
        if (jaegerEnabled) {
            JaegerTraceExporter.unregister();
        }
    }
}
