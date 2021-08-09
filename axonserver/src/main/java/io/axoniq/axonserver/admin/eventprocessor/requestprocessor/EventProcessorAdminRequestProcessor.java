package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.ClaimedSegmentState;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorState;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.component.processor.ClientsByEventProcessor;
import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.ProcessorEventPublisher;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

/**
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
    public void pause(@Nonnull EventProcessorId identifier, Authentication authentication) {
        String context = identifier.getContext();
        String processor = identifier.getName();
        String tokenStoreIdentifier = identifier.getTokenStoreIdentifier();
        auditLog.info("[{}@{}] Request to pause Event processor \"{}@{}\" in context \"{}\".",
                      authentication.getName(),
                      context,
                      processor,
                      tokenStoreIdentifier,
                      context);
        clientsByEventProcessor(context, processor, tokenStoreIdentifier)
                .forEach(clientId -> processorEventsSource
                        .pauseProcessorRequest(context, clientId, processor));
    }


    @Nonnull
    @Override
    public Flux<EventProcessorState> eventProcessorsForContext(@Nonnull String context, Authentication authentication) {
        auditLog.debug("[{}@{}] Request to list Event processors for context \"{}\".",
                       authentication.getName(), context, context);

        return Flux.fromIterable(eventProcessors)
                   .filter(eventProcessor -> eventProcessor.belongsToContext(context))
                   .map(eventProcessor -> {
                       String tokenStoreIdentifier = eventProcessor.eventProcessorInfo().getTokenStoreIdentifier();
                       String name = eventProcessor.eventProcessorInfo().getProcessorName();
                       EventProcessorId eventProcessorId = new EventProcessorId(name, context, tokenStoreIdentifier);
                       List<ClaimedSegmentState> claimedSegmentStates = new LinkedList<>(); //TODO populate the list
                       return new EventProcessorState(eventProcessorId, claimedSegmentStates);
                   });
    }


    private ClientsByEventProcessor clientsByEventProcessor(String context,
                                                            String processorName,
                                                            String tokenStoreIdentifier) {
        return new ClientsByEventProcessor(new EventProcessorIdentifier(processorName, tokenStoreIdentifier),
                                           context,
                                           eventProcessors);
    }
}
