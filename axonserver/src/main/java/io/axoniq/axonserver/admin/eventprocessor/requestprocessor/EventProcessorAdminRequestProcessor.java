package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.ClaimedSegmentState;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorState;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
@Service
public class EventProcessorAdminRequestProcessor implements EventProcessorAdminService {

    private static final Logger auditLog = AuditLog.getLogger();

    private final ProcessorEventPublisher processorEventsSource;
    private final ClientProcessors eventProcessors;

    public EventProcessorAdminRequestProcessor(
            ProcessorEventPublisher processorEventsSource,
            ClientProcessors eventProcessors) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
    }


    @Override
    public void pause(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to pause Event processor \"{}@{}\".",
                          authentication.name(),
                          processor,
                          tokenStoreIdentifier);
        }

        EventProcessorIdentifier id = new EventProcessorIdentifier(processor, tokenStoreIdentifier);
        Flux.fromIterable(eventProcessors)
            .filter(eventProcessor -> id.equals(new EventProcessorIdentifier(eventProcessor)))
            .map(ClientProcessor::clientId)
            .distinct()
            .subscribe(clientId -> processorEventsSource.pauseProcessorRequest(clientId, processor));
    }

    @Nonnull
    @Override
    public Flux<EventProcessorState> eventProcessors(@Nonnull Authentication authentication) {
        if (auditLog.isInfoEnabled()) {
            auditLog.debug("[{}] Request to list Event processors.", authentication.name());
        }
        return Flux.fromIterable(eventProcessors)
                   .groupBy(EventProcessorIdentifier::new)
                   .flatMap(this::eventProcessorState);
    }

    private Mono<EventProcessorState> eventProcessorState(
            GroupedFlux<EventProcessorIdentifier, ClientProcessor> clientProcessor) {
        return clientProcessor
                .reduce(new LinkedList<ClaimedSegmentState>(), (list, processor) -> {
                    processor.forEach(segment -> list.add(new SegmentState(segment, processor::clientId)));
                    return list;
                })
                .map(segments -> new EventProcessor(clientProcessor.key(), segments));
    }
}


class SegmentState implements ClaimedSegmentState {

    private final SegmentStatus segment;
    private final Supplier<String> clientId;

    public SegmentState(SegmentStatus segment, Supplier<String> clientId) {
        this.segment = segment;
        this.clientId = clientId;
    }

    @Nonnull
    @Override
    public String clientId() {
        return clientId.get();
    }

    @Override
    public int segmentId() {
        return segment.getSegmentId();
    }

    @Override
    public int onePartOf() {
        return segment.getOnePartOf();
    }
}


class Id implements EventProcessorId {

    private final EventProcessorIdentifier eventProcessorIdentifier;

    Id(EventProcessorIdentifier eventProcessorIdentifier) {
        this.eventProcessorIdentifier = eventProcessorIdentifier;
    }

    @Nonnull
    @Override
    public String name() {
        return eventProcessorIdentifier.name();
    }

    @Nonnull
    @Override
    public String tokenStoreIdentifier() {
        return eventProcessorIdentifier.tokenStoreIdentifier();
    }
}

class EventProcessor implements EventProcessorState {

    private final EventProcessorIdentifier eventProcessorIdentifier;
    private final List<ClaimedSegmentState> segments;

    EventProcessor(EventProcessorIdentifier eventProcessorIdentifier,
                   List<ClaimedSegmentState> segments) {
        this.eventProcessorIdentifier = eventProcessorIdentifier;
        this.segments = segments;
    }

    @Nonnull
    @Override
    public EventProcessorId identifier() {
        return new Id(eventProcessorIdentifier);
    }

    @Nonnull
    @Override
    public List<ClaimedSegmentState> segments() {
        return segments;
    }
}