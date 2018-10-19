package io.axoniq.axonserver.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

/**
 * Author: marc
 */
@Service
public class GraphiteMetricsServer implements SmartLifecycle{
    private final Logger logger = LoggerFactory.getLogger(GraphiteMetricsServer.class);
    private final String server;
    private final int port;
    private boolean running;



    public GraphiteMetricsServer(@Value("${management.metrics.export.graphite.host:localhost}") String server,
                                 @Value("${management.metrics.export.graphite.port:2004}") int port) {
        this.server = server;
        this.port = port;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        running = false;
        runnable.run();
    }

    @Override
    public void start() {
        logger.warn("Using Graphite integration, connecting to {}:{}", server, port);
    }

    @Override
    public void stop() {
        stop(()-> {});
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
