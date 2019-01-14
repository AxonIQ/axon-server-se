package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.metric.Metrics;
import io.axoniq.axonserver.metric.SnapshotMetric;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/**
 * @author Marc Gathier
 */
@Service("CommandMetricsRegistry")
public class CommandMetricsRegistry {
    private final Logger logger = LoggerFactory.getLogger(CommandMetricsRegistry.class);

    private final MeterRegistry meterRegistry;
    private final MetricCollector clusterMetrics;
    private final Map<String, Timer> timerMap = new ConcurrentHashMap<>();

    public CommandMetricsRegistry( MeterRegistry meterRegistry,
                                   MetricCollector clusterMetrics) {
        this.meterRegistry = meterRegistry;
        this.clusterMetrics = clusterMetrics;
    }


    public void add(String command, String clientId,  long duration) {
        try {
            timer(command, clientId).record(duration, TimeUnit.MILLISECONDS);
        } catch( Exception ex) {
            logger.debug("Failed to create timer", ex);
        }
    }

    private Timer timer(String command, String clientId) {
        String metricName = metricName(command, clientId);
        return timerMap.computeIfAbsent(metricName, name ->
                meterRegistry.timer(name));
    }

    private static String metricName(String command, String clientId) {
        return String.format("axon.command.%s.%s", command, clientId);
    }

    private ClusterMetric clusterMetric(String command, String clientId){
        String metricName = metricName(command, clientId);
        return new CompositeMetric(new SnapshotMetric(timer(command, clientId).takeSnapshot()), new Metrics(metricName, clusterMetrics));
    }

    public CommandMetric commandMetric(String command, String clientId, String componentName) {
        return new CommandMetric(command,clientId, componentName, clusterMetric(command, clientId).size());
    }

    public Counter counter(String commandCounterName) {
        return Counter.builder(commandCounterName).register(meterRegistry);
    }

    public <T> Gauge gauge(String activeCommandsGauge, T objectToWatch, ToDoubleFunction<T> gaugeFunction) {
        return Gauge.builder(activeCommandsGauge, objectToWatch, gaugeFunction)
             .register(meterRegistry);
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
