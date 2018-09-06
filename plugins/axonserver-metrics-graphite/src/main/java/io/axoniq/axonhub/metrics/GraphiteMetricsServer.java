package io.axoniq.axonhub.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
@Service
public class GraphiteMetricsServer implements SmartLifecycle{
    private final Logger logger = LoggerFactory.getLogger(GraphiteMetricsServer.class);
    private final MetricRegistry metrics;
    private GraphiteReporter reporter;
    private boolean running;

    @Value("${axoniq.axonhub.export.graphite.server:192.168.99.100}")
    private String server;
    @Value("${axoniq.axonhub.export.graphite.port:2003}")
    private int port;
    @Value("${axoniq.axonhub.export.graphite.period:60}")
    private int period;
    @Value("${axoniq.axonhub.name:name}")
    private String name;

    public GraphiteMetricsServer(MetricRegistry metrics) {
        this.metrics = metrics;
    }


    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        reporter.stop();
        running = false;
    }

    @Override
    public void start() {
        logger.info("Starting GraphiteMetricsServer, connecting to {}:{}", server, port);
        final Graphite graphite = new Graphite(new InetSocketAddress(server, port));
        reporter = GraphiteReporter.forRegistry(metrics)
                .prefixedWith(name)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        reporter.start(period, TimeUnit.SECONDS);

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
