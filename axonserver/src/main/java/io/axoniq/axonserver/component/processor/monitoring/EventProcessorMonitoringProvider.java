package io.axoniq.axonserver.component.processor.monitoring;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Mastermind for all event processor metrics. Any metrics should be provided by a bean implementing {@link EventProcessorMetricPublisher}.
 * The {@link EventProcessorMonitoringProvider} orchestrates the publishing of metrics, and instructs publishers to clean
 * up if a processor has not been seen in over an hour.
 *
 * @author Mitchell Herrijgers
 */
@Service
public class EventProcessorMonitoringProvider {
    private final Clock clock;
    private final ClientProcessors clientProcessors;
    private final List<EventProcessorMetricPublisher> publishers;

    private final Map<EventProcessorKey, Instant> registryMap = new ConcurrentHashMap<>();

    public EventProcessorMonitoringProvider(Clock clock,
                                            ClientProcessors clientProcessors,
                                            List<EventProcessorMetricPublisher> publishers) {
        this.clock = clock;
        this.clientProcessors = clientProcessors;
        this.publishers = publishers;
    }

    @EventListener
    public void on(EventProcessorEvents.EventProcessorStatusUpdated event) {
        final ClientEventProcessorInfo info = event.eventProcessorStatus();
        final EventProcessorKey key = new EventProcessorKey(
                info.getContext(),
                info.getComponent(),
                info.getEventProcessorInfo().getProcessorName()
        );

        registryMap.put(key, clock.instant());
        update(key);
    }

    private void update(final EventProcessorKey key) {
        final List<EventProcessorInfo> eventProcessorInfo = StreamSupport
                .stream(clientProcessors.spliterator(), false)
                .filter(p -> getEventProcessorKey(p).equals(key))
                .map(ClientProcessor::eventProcessorInfo)
                .filter(EventProcessorInfo::getRunning)
                .collect(Collectors.toList());
        publishers.forEach(p -> p.publishFor(key, eventProcessorInfo));
    }

    private EventProcessorKey getEventProcessorKey(final ClientProcessor p) {
        final String component = p.component();
        final String processorName = p.eventProcessorInfo().getProcessorName();
        final String context = p.context();
        return new EventProcessorKey(context, component, processorName);
    }

    @Scheduled(fixedRate = 60000)
    public void cleanup() {
        final Instant limit = clock.instant().minus(1, ChronoUnit.HOURS);
        registryMap.forEach((key, instant) -> {
            if (instant.isBefore(limit)) {
                publishers.forEach(p -> p.cleanup(key));
                registryMap.remove(key);
            }
        });
    }
}
