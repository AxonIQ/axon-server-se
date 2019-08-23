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
public class GrpcMetricsConfig {

    /**
     * Enables Axon Server gRPC metrics.
     */
    private boolean enabled = true;

    /**
     * Enables ZPages for displaying traces/stats.
     */
    private boolean enabledZPages = true;
    /**
     * HTTP port to access ZPages. Will not be considered if {@link #enabledZPages} is set to {@code false}.
     */
    private int zPagesPort = 8888;

    /**
     * Enables exporter for Prometheus.
     */
    private boolean enabledPrometheus = false;
    /**
     * HTTP port access Prometheus exporter. Will not be considered if {@link #enabledPrometheus} is set to {@code
     * false}.
     */
    private int prometheusPort = 8889;

    /**
     * Enables exporter for Jaeger.
     */
    private boolean enabledJaeger = false;
    /**
     * Endpoint to access Jaeger exporter. Will not be considered if {@link #enabledJaeger} is set to {@code false}.
     */
    private String jaegerEndpoint = "";
    /**
     * Service name to be set to Jaeger exporter. Will not be considered if {@link #enabledJaeger} is set to {@code
     * false}.
     */
    private String jaegerServiceName = "axonserver";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabledZPages() {
        return enabledZPages;
    }

    public void setEnabledZPages(boolean enabledZPages) {
        this.enabledZPages = enabledZPages;
    }

    public int getzPagesPort() {
        return zPagesPort;
    }

    public void setzPagesPort(int zPagesPort) {
        this.zPagesPort = zPagesPort;
    }

    public boolean isEnabledPrometheus() {
        return enabledPrometheus;
    }

    public void setEnabledPrometheus(boolean enabledPrometheus) {
        this.enabledPrometheus = enabledPrometheus;
    }

    public int getPrometheusPort() {
        return prometheusPort;
    }

    public void setPrometheusPort(int prometheusPort) {
        this.prometheusPort = prometheusPort;
    }

    public boolean isEnabledJaeger() {
        return enabledJaeger;
    }

    public void setEnabledJaeger(boolean enabledJaeger) {
        this.enabledJaeger = enabledJaeger;
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
