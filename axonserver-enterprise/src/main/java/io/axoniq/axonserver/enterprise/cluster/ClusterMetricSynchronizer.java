package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.Metric;
import io.axoniq.axonserver.grpc.internal.NodeMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Synchronizes Axon Server specific metrics to other Axon Server nodes.
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class ClusterMetricSynchronizer {

    private final Publisher<ConnectorCommand> clusterPublisher;

    private final MeterRegistry meterRegistry;
    private final String node;

    @Autowired
    public ClusterMetricSynchronizer(Publisher<ConnectorCommand> clusterPublisher,
                                     MeterRegistry meterRegistry,
                                     MessagingPlatformConfiguration configuration) {
        this(clusterPublisher, meterRegistry, configuration.getName());
    }

    public ClusterMetricSynchronizer(Publisher<ConnectorCommand> clusterPublisher,
                                     MeterRegistry meterRegistry,
                                     String node) {
        this.clusterPublisher = clusterPublisher;
        this.meterRegistry = meterRegistry;
        this.node = node;
    }


    @Scheduled(fixedRateString = "${axoniq.axonserver.metrics-synchronization-rate:15000}")
    public void shareMetrics() {
        NodeMetrics.Builder metrics = NodeMetrics.newBuilder().setNode(node);
        meterRegistry.forEachMeter(meter -> {
            // Id contains tags
            if( isAxonMeter(meter)) {
                if( meter instanceof Timer) {
                    HistogramSnapshot snapshot = ((Timer) meter)
                            .takeSnapshot();
                    metrics.addMetrics(
                            Metric.newBuilder().setName(meter.getId().getName())
                                  .setCount(snapshot.count())
                                  .setMean(snapshot.mean())
                                  .setValue(snapshot.mean())
                                  .setMedian(getPercentile(snapshot, 0.5))
                                  .setPercentile95(getPercentile(snapshot, 0.95))
                                  .setPercentile99(getPercentile(snapshot, 0.99))
                                  .setMin(0L)
                                  .putAllTags(tags(meter.getId().getTags()))
                                  .setMax(snapshot.max())
                    );
                }
                else if( meter instanceof Counter) {
                    metrics.addMetrics(
                            Metric.newBuilder().setName(meter.getId().getName())
                                  .putAllTags(tags(meter.getId().getTags()))
                                  .setCount((long) ((Counter) meter).count())
                    );
                } else if( meter instanceof Gauge) {
                        metrics.addMetrics(
                                Metric.newBuilder().setName(meter.getId().getName())
                                      .putAllTags(tags(meter.getId().getTags()))
                                      .setValue(((Gauge) meter).value())
                        );
                }
            }
        });
        clusterPublisher.publish(ConnectorCommand.newBuilder()
                                                 .setMetrics(metrics).build());
    }

    private Map<String, String> tags(List<Tag> tags) {
        Map<String, String> tagsMap = new HashMap<>();
        tags.forEach(t -> tagsMap.put(t.getKey(), t.getValue()));
        return tagsMap;
    }

    private double getPercentile(HistogramSnapshot snapshot, double percentile) {
        for (ValueAtPercentile valueAtPercentile : snapshot.percentileValues()) {
            if( valueAtPercentile.percentile() == percentile) return valueAtPercentile.value();
        }
        return 0d;
    }

    private boolean isAxonMeter(Meter meter) {
        return meter.getId().getName().startsWith("axon.");
    }
}
