package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.metric.AxonServerMetric;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.metric.NodeMetric;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.StreamSupport.stream;

/**
 * @author Marc Gathier
 */
public class FakeMetricCollector implements MetricCollector {

    final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Override
    public Iterable<AxonServerMetric> getAll(String metricName, Tags tags) {
        return meterRegistry.find(metricName).tags(tags).meters().stream().map(this::asAxonServerMetric)
                            .collect(Collectors.toList());
    }

    private AxonServerMetric asAxonServerMetric(Meter meter) {
        return new AxonServerMetric() {
            @Override
            public double getValue() {
                if (meter instanceof Counter) {
                    return ((Counter) meter).count();
                }
                if (meter instanceof Gauge) {
                    return ((Gauge) meter).value();
                }
                if (meter instanceof Timer) {
                    return ((Timer) meter).totalTime(TimeUnit.MILLISECONDS);
                }
                return -1;
            }

            @Override
            public double getMin() {
                return 0;
            }

            @Override
            public double getMax() {
                if (meter instanceof Timer) {
                    return ((Timer) meter).max(TimeUnit.MILLISECONDS);
                }
                return 0;
            }

            @Override
            public double getMean() {
                if (meter instanceof Timer) {
                    return ((Timer) meter).mean(TimeUnit.MILLISECONDS);
                }
                return 0;
            }

            @Override
            public String getName() {
                return meter.getId().getName();
            }

            @Override
            public long getCount() {
                if (meter instanceof Timer) {
                    return ((Timer) meter).count();
                }
                return 0;
            }
        };
    }

    @Override
    public ClusterMetric apply(String metricName, Tags tags) {
        Set<ClusterMetric> metrics = stream(getAll(metricName, tags).spliterator(), false)
                .map(NodeMetric::new)
                .collect(Collectors.toSet());
        return new CompositeMetric(metrics);
    }

    public void gauge(String metric, Tags tags, int value) {
        meterRegistry.gauge(metric, tags, new AtomicInteger(value));
    }
}
