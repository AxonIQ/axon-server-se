package io.axoniq.axonserver.component.processor.monitoring;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

import java.util.List;

/**
 * Represents a component that publishes metrics for event processors, based on the {@link EventProcessorKey}.
 *
 * @author Mitchell Herrijgers
 */
public interface EventProcessorMetricPublisher {
    /**
     * The implementing class should publish the result of a computation to a metric, such as a
     * {@link io.micrometer.core.instrument.Gauge} or {@link io.micrometer.core.instrument.Counter}.
     * <p>
     * All relevant instances of {@link EventProcessorInfo} are provided to execute this computation.
     *
     * @param key        The key, unique for each event processor.
     * @param processors The processor info's to base the computation on.
     */
    void publishFor(EventProcessorKey key, List<EventProcessorInfo> processors);

    /**
     * The implementing class should clean up any lingering data for this processor. Called when an event processor is not seen for over an hour.
     *
     * @param key The key, unique for each event processor.
     */
    void cleanup(EventProcessorKey key);
}
