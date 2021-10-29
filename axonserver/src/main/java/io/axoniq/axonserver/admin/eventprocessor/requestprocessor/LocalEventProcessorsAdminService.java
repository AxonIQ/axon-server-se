package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * Service that implements the operations applicable to an Event Processor.
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
@Service
public class LocalEventProcessorsAdminService implements EventProcessorAdminService {

    private static final Logger auditLog = AuditLog.getLogger();
    private final ProcessorEventPublisher processorEventsSource;
    private final ClientProcessors eventProcessors;

    /**
     * Default implementation of {@link EventProcessorAdminService}.
     *
     * @param processorEventsSource used to propagate the instructions to the proper clients
     * @param eventProcessors       the list of all event processors
     */
    public LocalEventProcessorsAdminService(
            ProcessorEventPublisher processorEventsSource,
            ClientProcessors eventProcessors) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
    }

    @Nonnull
    @Override
    public Mono<Void> pause(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to pause Event processor \"{}@{}\".",
                          AuditLog.username(authentication.username()),
                          processor,
                          tokenStoreIdentifier);
        }
        return Flux.fromIterable(eventProcessors)
                   .filter(eventProcessor -> new EventProcessorIdentifier(eventProcessor).equals(identifier))
                   .doOnNext(ep -> processorEventsSource.pauseProcessorRequest(ep.context(), ep.clientId(), processor))
                   .then();
        // the context will be removed from the event processor
    }

    /**
     * Handles a request to start a certain event processor.
     * The method returns once the request has been propagated to the proper clients.
     * It doesn't imply that the clients have processed it.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    @Override
    public void start(@Nonnull EventProcessorId identifier, @Nonnull Authentication authentication) {
        String processor = identifier.name();
        String tokenStoreIdentifier = identifier.tokenStoreIdentifier();
        if (auditLog.isInfoEnabled()) {
            auditLog.info("[{}] Request to start Event processor \"{}@{}\".",
                          AuditLog.username(authentication.name()),
                          processor,
                          tokenStoreIdentifier);
        }

        EventProcessorIdentifier id = new EventProcessorIdentifier(processor, tokenStoreIdentifier);
        Flux.fromIterable(eventProcessors)
            .filter(eventProcessor -> id.equals(new EventProcessorIdentifier(eventProcessor)))
            .subscribe(ep -> processorEventsSource.startProcessorRequest(ep.context(), ep.clientId(), processor));
        // the context will be removed from the event processor
    }

