package io.axoniq.axonhub.cluster;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.grpc.Publisher;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.Metric;
import io.axoniq.axonhub.internal.grpc.NodeMetrics;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by Sara Pellegrini on 17/04/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ClusterMetricSynchronizer {

    private final MetricRegistry registry;

    private final Publisher<ConnectorCommand> clusterPublisher;

    private final MessagingPlatformConfiguration configuration;

    public ClusterMetricSynchronizer(MetricRegistry registry,
                                     Publisher<ConnectorCommand> clusterPublisher,
                                     MessagingPlatformConfiguration configuration) {
        this.registry = registry;
        this.clusterPublisher = clusterPublisher;
        this.configuration = configuration;
    }


    @Scheduled(fixedRateString = "${axoniq.axonhub.metrics-synchronization-rate}")
    public void shareMetrics() {
        NodeMetrics.Builder metrics = NodeMetrics.newBuilder().setNode(configuration.getName());
        registry.getHistograms().forEach(
                (metric, histogram) -> {
                    Snapshot snapshot = histogram.getSnapshot();
                    metrics.addMetrics(
                            Metric.newBuilder().setName(metric)
                                  .setSize(snapshot.size())
                                  .setMean(snapshot.getMean())
                                  .setMin(snapshot.getMin())
                                  .setMax(snapshot.getMax())
                                  .setMedian(snapshot.getMedian())
                                  .setPercentile75(snapshot.get75thPercentile())
                                  .setPercentile95(snapshot.get95thPercentile())
                                  .setPercentile98(snapshot.get98thPercentile())
                                  .setPercentile99(snapshot.get99thPercentile())
                                  .setPercentile999(snapshot.get999thPercentile())
                                  );
                });

        registry.getCounters().forEach(
                (metric, counter) -> metrics.addMetrics(
                        Metric.newBuilder().setName(metric).setSize(counter.getCount())));


        clusterPublisher.publish(ConnectorCommand.newBuilder().setMetrics(metrics).build());
    }
}
