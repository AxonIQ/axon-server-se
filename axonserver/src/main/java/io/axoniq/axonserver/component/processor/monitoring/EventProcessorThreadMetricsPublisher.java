package io.axoniq.axonserver.component.processor.monitoring;

import com.google.common.util.concurrent.AtomicDouble;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.axoniq.axonserver.component.processor.monitoring.EventProcessorMonitoringUtils.createMetric;

/**
 * Publishes two metrics:
 * - {@link BaseMetricName#AXON_EVENT_PROCESSOR_THREADS_ACTIVE}: The amount of threads active for this processor.
 * - {@link BaseMetricName#AXON_EVENT_PROCESSOR_THREADS_AVAILABLE}: The amount of threads available for this processor.
 * <p>
 * These metrics can be used together in monitoring to visualize the amount of active segments, as well as the amount
 * of segments that can still be claimed. The {@link BaseMetricName#AXON_EVENT_PROCESSOR_THREADS_AVAILABLE} is excluded
 * for Pooled Streaming event processors, since these have a (nearly) unlimited amount of segments, throwing off monitoring.
 *
 * @author Mitchell Herrijgers
 */
@Component
public class EventProcessorThreadMetricsPublisher implements EventProcessorMetricPublisher {
    private final MeterFactory meterFactory;
    private final Map<EventProcessorKey, AtomicDouble> activeMap = new HashMap<>();
    private final Map<EventProcessorKey, AtomicDouble> availableMap = new HashMap<>();

    public EventProcessorThreadMetricsPublisher(final MeterFactory meterFactory) {
        this.meterFactory = meterFactory;
    }

    @Override
    public void publishFor(final EventProcessorKey key, final List<EventProcessorInfo> processors) {
        AtomicDouble activeMetric = activeMap.computeIfAbsent(key, k -> createMetric(k, BaseMetricName.AXON_EVENT_PROCESSOR_THREADS_ACTIVE, meterFactory));
        int activeThreads = processors.stream().mapToInt(EventProcessorInfo::getActiveThreads).sum();
        activeMetric.set(activeThreads);

        final boolean isPooledStreaming = processors.stream().anyMatch(p -> p.getMode().equals("Pooled Streaming"));
        if (!isPooledStreaming) {
            AtomicDouble availableMetric = availableMap.computeIfAbsent(key, k -> createMetric(k, BaseMetricName.AXON_EVENT_PROCESSOR_THREADS_AVAILABLE, meterFactory));
            int availableThreads = processors.stream().mapToInt(EventProcessorInfo::getAvailableThreads).sum();
            availableMetric.set(availableThreads);
        }
    }

    @Override
    public void cleanup(final EventProcessorKey key) {
        activeMap.remove(key);
        availableMap.remove(key);
    }
}
