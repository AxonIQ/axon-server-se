package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;

/**
 * Responsible to refresh the event processor status any time an operations of split or merge has been performed.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class EventProcessorResultListener {

    public interface ProcessorProvider {
        EventProcessorIdentifier get(String context, String client, String tokenStoreIdentifier);
    }

    private final BiConsumer<String, EventProcessorIdentifier> refreshOperation;

    private final ProcessorProvider eventProcessorIdentifierProvider;

    /**
     * Creates an instance of {@link EventProcessorResultListener} based on the {@link EventProcessorStatusRefresh}.
     *
     * @param refreshEventProcessorStatus      used to require a refresh of the status of the event processors.
     * @param eventProcessorIdentifierProvider used to retrieve the token store identifier for the specified processor
     *                                         name and client name
     */
    @Autowired
    public EventProcessorResultListener(EventProcessorStatusRefresh refreshEventProcessorStatus,
                                        EventProcessorIdentifierProvider eventProcessorIdentifierProvider) {
        this(refreshEventProcessorStatus::run, eventProcessorIdentifierProvider::get);
    }

    /**
     * Creates an instance of {@link EventProcessorResultListener} based on the specified refresh operation.
     *
     * @param refreshOperation                 used to require a refresh of the status of the event processors.
     * @param eventProcessorIdentifierProvider used to retrieve the token store identifier for the specified processor
     *                                         name and client name
     */
    public EventProcessorResultListener(BiConsumer<String, EventProcessorIdentifier> refreshOperation,
                                        ProcessorProvider eventProcessorIdentifierProvider) {
        this.refreshOperation = refreshOperation;
        this.eventProcessorIdentifierProvider = eventProcessorIdentifierProvider;
    }

    /**
     * Refresh the state of the event processor after a merge operation has been performed.
     *
     * @param event the event describing the event processor that has been merged
     */
    @EventListener
    public void on(EventProcessorEvents.MergeSegmentsSucceeded event) {
        refresh(event.context(), event.clientId(), event.processorName());
    }

    /**
     * Refresh the state of the event processor after a slit operation has been performed.
     *
     * @param event the event describing the event processor that has been split
     */
    @EventListener
    public void on(EventProcessorEvents.SplitSegmentsSucceeded event) {
        refresh(event.context(), event.clientId(), event.processorName());
    }

    private void refresh(String context, String clientName, String processorName) {
        EventProcessorIdentifier processor = eventProcessorIdentifierProvider.get(context, clientName, processorName);
        refreshOperation.accept(context, processor);
    }
}
