package io.axoniq.axonserver.component.processor.monitoring;

import com.google.common.util.concurrent.AtomicDouble;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.*;

import static io.axoniq.axonserver.component.processor.monitoring.EventProcessorMonitoringUtils.createMetric;

/**
 * Publishes the {@link BaseMetricName#AXON_EVENT_PROCESSOR_SEGMENTS_UNCLAIMED} metric. If this value is higher than 0,
 * segments are unclaimed and events are not being handled by the projection.
 * <p>
 * This metric can be used to detect event processors in error, being stuck or not having enough threads available.
 *
 * @author Mitchell Herrijgers
 */
@Component
public class EventProcessorSegmentMetricsPublisher implements EventProcessorMetricPublisher {
    private final MeterFactory meterFactory;
    private final Map<EventProcessorKey, AtomicDouble> unclaimedMap = new HashMap<>();

    public EventProcessorSegmentMetricsPublisher(final MeterFactory meterFactory) {
        this.meterFactory = meterFactory;
    }

    @Override
    public void publishFor(final EventProcessorKey key, final List<EventProcessorInfo> processors) {
        final AtomicDouble metric = unclaimedMap.computeIfAbsent(key, k -> createMetric(k, BaseMetricName.AXON_EVENT_PROCESSOR_SEGMENTS_UNCLAIMED, meterFactory));
        metric.set(determineUnclaimedSegments(processors));
    }

    private double determineUnclaimedSegments(final List<EventProcessorInfo> processors) {
        Set<Integer> ids = new HashSet<>();
        final double claimedSegments = processors.stream()
                .map(EventProcessorInfo::getSegmentStatusList)
                .flatMap(Collection::stream)
                .filter(c -> StringUtils.isEmpty(c.getErrorState()))
                .mapToDouble(segmentStatus -> {
                    int segmentId = segmentStatus.getSegmentId();
                    if (ids.contains(segmentId)) {
                        return 0d;
                    }
                    ids.add(segmentId);
                    return 1d / segmentStatus.getOnePartOf();
                })
                .sum();
        return 1 - claimedSegments;
    }


    @Override
    public void cleanup(final EventProcessorKey key) {
        unclaimedMap.remove(key);
    }
}
