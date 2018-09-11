package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.Metric;
import io.axoniq.axonhub.internal.grpc.NodeMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by Sara Pellegrini on 17/04/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ClusterMetricSynchronizer {

    private final Publisher<ConnectorCommand> clusterPublisher;

    private final MeterRegistry meterRegistry;
    private final MessagingPlatformConfiguration configuration;

    public ClusterMetricSynchronizer(Publisher<ConnectorCommand> clusterPublisher,
                                     MeterRegistry meterRegistry,
                                     MessagingPlatformConfiguration configuration) {
        this.clusterPublisher = clusterPublisher;
        this.meterRegistry = meterRegistry;
        this.configuration = configuration;
    }


    @Scheduled(fixedRateString = "${axoniq.axonserver.metrics-synchronization-rate}")
    public void shareMetrics() {
        NodeMetrics.Builder metrics = NodeMetrics.newBuilder().setNode(configuration.getName());
        meterRegistry.forEachMeter(meter -> {
            if( isAxonMeter(meter)) {
                if( meter instanceof Timer) {
                    HistogramSnapshot snapshot = ((Timer) meter)
                            .takeSnapshot();
                    metrics.addMetrics(
                            Metric.newBuilder().setName(meter.getId().getName())
                                  .setSize(snapshot.count())
                                  .setMean(snapshot.mean())
                                  .setMedian(getPercentile(snapshot, 0.5))
                                  .setPercentile95(getPercentile(snapshot, 0.95))
                                  .setPercentile99(getPercentile(snapshot, 0.99))
                                  .setMin(0L)
                                  .setMax((long)snapshot.max())
                    );
                }
                else if( meter instanceof Counter) {
                    metrics.addMetrics(
                            Metric.newBuilder().setName(meter.getId().getName())
                                  .setSize((long) ((Counter) meter).count())
                    );
                } else if( meter instanceof Gauge) {
                        metrics.addMetrics(
                                Metric.newBuilder().setName(meter.getId().getName())
                                      .setSize((long) ((Gauge)meter).value())
                        );
                }
            }
        });
        clusterPublisher.publish(ConnectorCommand.newBuilder().setMetrics(metrics).build());
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
