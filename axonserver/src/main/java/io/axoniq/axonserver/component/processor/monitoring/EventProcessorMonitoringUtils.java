package io.axoniq.axonserver.component.processor.monitoring;

import com.google.common.util.concurrent.AtomicDouble;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;

/**
 * Utilities for use in event processor monitoring.
 *
 * @author Mitchell Herrijgers
 */
public interface EventProcessorMonitoringUtils {
    /**
     * Creates an {@link AtomicDouble} and registers it to the provided {@link MeterFactory} with the {@link BaseMetricName}.
     *
     * @param key          Key of the processor, to create tags.
     * @param metric       The {@link BaseMetricName} to register.
     * @param meterFactory The {@link MeterFactory} to use.
     *
     * @return An {@link AtomicDouble}, watched by the metric implementation. Each {@link AtomicDouble#set(double)}
     * will be reflected in the metrics.
     */
    static AtomicDouble createMetric(final EventProcessorKey key, final BaseMetricName metric, final MeterFactory meterFactory) {
        final AtomicDouble atomicDouble = new AtomicDouble();
        meterFactory.gauge(metric, key.asMetricTags(), atomicDouble, AtomicDouble::get);
        return atomicDouble;
    }
}
