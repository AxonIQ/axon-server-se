package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.internal.Metric;
import io.axoniq.axonserver.grpc.internal.NodeMetrics;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.micrometer.core.instrument.Tags;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ClusterMetricTargetTest {

    private ClusterMetricTarget testSubject = new ClusterMetricTarget();

    @Test
    public void apply() {
        NodeMetrics.Builder metricsBuilder = NodeMetrics.newBuilder().setNode("node1");
        metricsBuilder.addMetrics(Metric.newBuilder()
                                        .setName("sample")
                                        .setValue(10)
                                        .putTags("context", "context1")
                                        .putTags("target", "target1"));

        metricsBuilder.addMetrics(Metric.newBuilder()
                                        .setName("sample")
                                        .setValue(5)
                                        .putTags("context", "context1")
                                        .putTags("target", "target2"));

        MetricsEvents.MetricsChanged metricsChangedEvent = new MetricsEvents.MetricsChanged(metricsBuilder.build());
        testSubject.on(metricsChangedEvent);

        ClusterMetric clusterMetric = testSubject.apply("sample", Tags.empty());
        assertEquals(15, clusterMetric.value());

        clusterMetric = testSubject.apply("sample", Tags.of("target", "target2"));
        assertEquals(5, clusterMetric.value());

        clusterMetric = testSubject.apply("sample", Tags.of("context", "context1"));
        assertEquals(15, clusterMetric.value());
    }
}