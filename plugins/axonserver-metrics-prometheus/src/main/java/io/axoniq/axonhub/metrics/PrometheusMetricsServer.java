package io.axoniq.axonserver.metrics;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;



/**
 * Author: marc
 */
@Service
public class PrometheusMetricsServer implements SmartLifecycle {
    private boolean running;
    private final MetricRegistry metrics;
    private DropwizardExports collector;

    public PrometheusMetricsServer(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        if( running) {
            CollectorRegistry.defaultRegistry.unregister(collector);
            running = false;
        }
    }

    @Override
    public void start() {
        collector = new DropwizardExports(metrics);
        CollectorRegistry.defaultRegistry.register(collector);
        running = true;
    }

    @Override
    public void stop() {
        stop( () -> {});
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 80;
    }
}
