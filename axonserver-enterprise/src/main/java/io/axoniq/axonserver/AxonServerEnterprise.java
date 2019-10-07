package io.axoniq.axonserver;

import io.axoniq.axonserver.cluster.grpc.LeaderElectionService;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.licensing.LicenseConfiguration;
import io.axoniq.axonserver.licensing.LicenseException;
import io.axoniq.axonserver.rest.PluginImportSelector;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import javax.annotation.PreDestroy;

/**
 * @author Marc Gathier
 */
@SpringBootApplication
@EnableAsync
@EnableScheduling
@Import(PluginImportSelector.class)
public class AxonServerEnterprise {

    private static final Logger log = LoggerFactory.getLogger(AxonServerEnterprise.class);

    public static void main(String[] args) {
        try {
            LicenseConfiguration.getInstance();
        } catch (LicenseException ex) {
            log.error(ex.getMessage());
            System.exit(-1);
        }
        System.setProperty("spring.config.name", "axonserver");
        SpringApplication.run(AxonServerEnterprise.class, args);
    }

    @Autowired
    public void configureGrpcMetrics(GrpcMetricsConfig metricsConfig, PrometheusMeterRegistry prometheusMeterRegistry)
            throws IOException {
        if (metricsConfig.isEnabled()) {
            TraceConfig traceConfig = Tracing.getTraceConfig();
            traceConfig.updateActiveTraceParams(
                    traceConfig.getActiveTraceParams()
                               .toBuilder()
                               .setSampler(Samplers.alwaysSample())
                               .build());
            // we are using a deprecated method here since exporters are not updated yet to work with RpcViews.registerAllGrpcViews();
            RpcViews.registerAllViews();

            if (metricsConfig.iszPagedEnabled()) {
                ZPageHandlers.startHttpServerAndRegisterAll(metricsConfig.getzPagesPort());
            }

            if (metricsConfig.isPrometheusEnabled()) {
                PrometheusStatsCollector.createAndRegister(PrometheusStatsConfiguration.builder()
                                                                                       .setRegistry(
                                                                                               prometheusMeterRegistry
                                                                                                       .getPrometheusRegistry())
                                                                                       .build());
            }

            if (metricsConfig.isJaegerEnabled()) {
                JaegerExporterConfiguration jaegerConf =
                        JaegerExporterConfiguration.builder()
                                                   .setServiceName(metricsConfig.getJaegerServiceName())
                                                   .setThriftEndpoint(metricsConfig.getJaegerEndpoint())
                                                   .build();
                JaegerTraceExporter.createAndRegister(jaegerConf);
            }
        }
    }

    @Bean
    public LogReplicationService logReplicationService(GrpcRaftController grpcRaftController) {
        return new LogReplicationService(grpcRaftController);
    }

    @Bean
    public LeaderElectionService leaderElectionService(GrpcRaftController grpcRaftController) {
        return new LeaderElectionService(grpcRaftController);
    }

    @PreDestroy
    public void clean() {
        GrpcFlowControlledDispatcherListener.shutdown();
        JaegerTraceExporter.unregister();
    }
}
