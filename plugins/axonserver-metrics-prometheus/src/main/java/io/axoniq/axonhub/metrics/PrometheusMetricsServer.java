package io.axoniq.axonhub.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;


/**
 * Author: marc
 */
@Service
public class PrometheusMetricsServer implements SmartLifecycle {
    private final Logger logger = LoggerFactory.getLogger(PrometheusMetricsServer.class);
    private boolean running;

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        if( running) {
            running = false;
        }
    }

    @Override
    public void start() {
        logger.warn("Using PrometheusMetrics integration");
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
