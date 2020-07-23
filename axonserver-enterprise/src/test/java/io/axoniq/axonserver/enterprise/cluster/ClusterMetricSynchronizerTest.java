package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.Metric;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import com.google.common.util.concurrent.AtomicDouble;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ClusterMetricSynchronizerTest {

    private MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private List<ConnectorCommand> commands = new ArrayList<>();
    private ClusterMetricSynchronizer testSubject = new ClusterMetricSynchronizer(c -> commands.add(c),
                                                                                  meterRegistry,
                                                                                  "currentNode");

    @Test
    public void shareTimers() {
        meterRegistry.timer("axon.timer", "context", "context1").record(Duration.of(12, ChronoUnit.MILLIS));
        meterRegistry.timer("axon.timer", "context", "context1").record(Duration.of(10, ChronoUnit.MILLIS));
        meterRegistry.timer("axon.timer", "context", "context1").record(Duration.of(1, ChronoUnit.MILLIS));
        meterRegistry.timer("axon.timer", "context", "context1").record(Duration.of(19, ChronoUnit.MILLIS));
        meterRegistry.timer("axon.timer", "context", "context1").record(Duration.of(23, ChronoUnit.MILLIS));
        meterRegistry.timer("axon.timer", "context", "context1").record(Duration.of(43, ChronoUnit.MILLIS));
        meterRegistry.timer("axon.timer", "context", "context2").record(Duration.of(23, ChronoUnit.MILLIS));
        testSubject.shareMetrics();

        assertEquals(1, commands.size());

        ConnectorCommand result = commands.get(0);
        System.out.println(result.getMetrics().getMetrics(0));
        assertEquals(2, result.getMetrics().getMetricsCount());
    }

    @Test
    public void shareGauge() {
        meterRegistry.gauge("axon.gauge", Tags.of("context", "context1"), new AtomicDouble(1.34d));
        testSubject.shareMetrics();

        assertEquals(1, commands.size());

        ConnectorCommand result = commands.get(0);
        assertEquals(1, result.getMetrics().getMetricsCount());

        Metric metric = result.getMetrics().getMetrics(0);
        System.out.println(metric);
        assertEquals(1.34d, metric.getValue(), 0);
        assertEquals("context1", metric.getTagsMap().get("context"));
    }

    @Test
    public void shareCounter() {
        meterRegistry.counter("axon.gauge", Tags.of("context", "context2")).increment();
        testSubject.shareMetrics();

        assertEquals(1, commands.size());

        ConnectorCommand result = commands.get(0);
        assertEquals(1, result.getMetrics().getMetricsCount());

        Metric metric = result.getMetrics().getMetrics(0);
        assertEquals(1, metric.getCount());
        assertEquals("context2", metric.getTagsMap().get("context"));
    }
}