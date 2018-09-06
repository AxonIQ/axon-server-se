package io.axoniq.axonhub.message.command;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistry.MetricSupplier;
import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.cluster.ClusterMetricTarget;
import io.axoniq.axonhub.metric.ClusterMetric;
import io.axoniq.axonhub.metric.CompositeMetric;
import io.axoniq.axonhub.metric.Metrics;
import io.axoniq.axonhub.metric.SnapshotMetric;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Author: marc
 */
@Service("CommandMetricsRegistry")
public class CommandMetricsRegistry {
    private final MetricRegistry metricRegistry;
    private final MetricSupplier<Histogram> histogramFactory;
    private final ClusterMetricTarget clusterMetrics;

    public CommandMetricsRegistry(MetricRegistry metricRegistry,
                                  MetricSupplier<Histogram> histogramFactory,
                                  ClusterMetricTarget clusterMetrics) {
        this.metricRegistry = metricRegistry;
        this.histogramFactory = histogramFactory;
        this.clusterMetrics = clusterMetrics;
    }


    public void add(String command, String clientId,  long duration) {
        try {
            histogram(command, clientId).update(duration);
        } catch( Exception ignore) {
        }
    }

    private Histogram histogram(String command, String clientId) {
        return metricRegistry.histogram(metricName(command, clientId), histogramFactory);
    }

    private String metricName(String command, String clientId) {
        return name("command", command, clientId);
    }

    private ClusterMetric clusterMetric(String command, String clientId){
        String metricName = metricName(command, clientId);
        return new CompositeMetric(new SnapshotMetric(histogram(command, clientId).getSnapshot()), new Metrics(metricName, clusterMetrics));
    }

    public CommandMetric commandMetric(String command, String clientId, String componentName) {
        return new CommandMetric(command,clientId, componentName, clusterMetric(command, clientId).size());
    }

    public void register(String name,
                      Gauge supplier) {
        metricRegistry.register(name, supplier);
    }

    long dispatchedCount(){
        String name = "commands";
        return metricRegistry.counter(name).getCount() + new CompositeMetric(new Metrics(name, clusterMetrics)).size();
    }

    void increaseDispatchedCount(){
        metricRegistry.counter("commands").inc();
    }



    @KeepNames
    public static class CommandMetric {
        private final String command;
        private final String clientId;
        private final String componentName;
        private final long count;

        CommandMetric(String command, String clientId, String componentName, long count) {
            this.command = command;
            this.clientId = clientId;
            this.componentName = componentName;
            this.count = count;
        }

        public String getCommand() {
            return command;
        }

        public String getClientId() {
            return clientId;
        }

        public String getComponentName() {
            return componentName;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CommandMetric that = (CommandMetric) o;
            return Objects.equals(command, that.command) &&
                    Objects.equals(clientId, that.clientId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(command, clientId);
        }
    }
}
