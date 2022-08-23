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
 * - {@link BaseMetricName#AXON_EVENT_PROCESSOR_EVENT_POSITION}: The lowest token position across all claimed segments of this processor.
 * - {@link BaseMetricName#AXON_EVENT_PROCESSOR_EVENT_LAG}: The highest amount any claimed segment is behind the head of this processor.
 * <p>
 * These metrics can be used together in monitoring to find a non-performing projection, or track how far a replay currently is.
 *
 * @author Mitchell Herrijgers
 */
@Component
public class EventProcessorTokenMetricsPublisher implements EventProcessorMetricPublisher {
    private final EventDispatcher eventDispatcher;
    private final MeterFactory meterFactory;
    private final Map<EventProcessorKey, AtomicDouble> lowestTokenPositionMap = new HashMap<>();
    private final Map<EventProcessorKey, AtomicDouble> eventLagMap = new HashMap<>();

    public EventProcessorTokenMetricsPublisher(final EventDispatcher eventDispatcher, final MeterFactory meterFactory) {
        this.eventDispatcher = eventDispatcher;
        this.meterFactory = meterFactory;
    }

    @Override
    public void publishFor(final EventProcessorKey key, final List<EventProcessorInfo> processors) {
        final AtomicDouble tokenPositionMetric = lowestTokenPositionMap.computeIfAbsent(key, k -> createMetric(k, BaseMetricName.AXON_EVENT_PROCESSOR_EVENT_POSITION, meterFactory));
        final AtomicDouble eventLagMetric = eventLagMap.computeIfAbsent(key, k -> createMetric(k, BaseMetricName.AXON_EVENT_PROCESSOR_EVENT_LAG, meterFactory));

        long lowestTokenPosition = determineLowestTokenPosition(processors);
        final long headToken = eventDispatcher.getNrOfEvents(key.getContext());
        final long eventLag = headToken - lowestTokenPosition;

        tokenPositionMetric.set(lowestTokenPosition);
        eventLagMetric.set(eventLag);
    }

    @Override
    public void cleanup(final EventProcessorKey key) {
        lowestTokenPositionMap.remove(key);
        eventLagMap.remove(key);
    }

    private long determineLowestTokenPosition(final List<EventProcessorInfo> processors) {
        return processors.stream()
                .map(EventProcessorInfo::getSegmentStatusList)
                .flatMap(Collection::stream)
                .mapToLong(EventProcessorInfo.SegmentStatus::getTokenPosition)
                .min()
                .orElse(0);
    }
}
