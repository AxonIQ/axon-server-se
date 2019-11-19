package io.axoniq.axonserver;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for gRPC metrics.
 *
 * @author Milan Savic
 * @since 4.2.1
 */
@ConfigurationProperties(prefix = "axoniq.axonserver.metrics.grpc")
@Configuration
public class GrpcMonitoringProperties {

    /**
     * Enables Axon Server gRPC metrics.
     */
    private boolean enabled = false;

    /**
     * Enables ZPages for displaying traces/stats.
     */
    private boolean zPagedEnabled = false;
    /**
     * HTTP port to access ZPages. Will not be considered if {@link #zPagedEnabled} is set to {@code false}.
     */
    private int zPagesPort = 8888;

    /**
     * Enables exporter for Prometheus.
     */
    private boolean prometheusEnabled = false;

    /**
     * Enables exporter for Jaeger.
     */
    private boolean jaegerEnabled = false;
    /**
     * Endpoint to access Jaeger exporter. Will not be considered if {@link #jaegerEnabled} is set to {@code false}.
     */
    private String jaegerEndpoint = "";
    /**
     * Service name to be set to Jaeger exporter. Will not be considered if {@link #jaegerEnabled} is set to {@code
     * false}.
     */
    private String jaegerServiceName = "axonserver";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean iszPagedEnabled() {
        return zPagedEnabled;
    }

    public void setzPagedEnabled(boolean zPagedEnabled) {
        this.zPagedEnabled = zPagedEnabled;
    }

    public int getzPagesPort() {
        return zPagesPort;
    }

    public void setzPagesPort(int zPagesPort) {
        this.zPagesPort = zPagesPort;
    }

    public boolean isPrometheusEnabled() {
        return prometheusEnabled;
    }

    public void setPrometheusEnabled(boolean prometheusEnabled) {
        this.prometheusEnabled = prometheusEnabled;
    }

    public boolean isJaegerEnabled() {
        return jaegerEnabled;
    }

    public void setJaegerEnabled(boolean jaegerEnabled) {
        this.jaegerEnabled = jaegerEnabled;
    }

    public String getJaegerEndpoint() {
        return jaegerEndpoint;
    }

    public void setJaegerEndpoint(String jaegerEndpoint) {
        this.jaegerEndpoint = jaegerEndpoint;
    }

    public String getJaegerServiceName() {
        return jaegerServiceName;
    }

    public void setJaegerServiceName(String jaegerServiceName) {
        this.jaegerServiceName = jaegerServiceName;
    }
}
