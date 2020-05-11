package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.component.instance.ClientContextProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Responsible to refresh the event processor status any time an operations of split or merge has been performed.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class EventProcessorResultListener {

    private final BiConsumer<String, EventProcessorIdentifier> refreshOperation;

    private final BiFunction<String, String, EventProcessorIdentifier> eventProcessorIdentifierProvider;

    private final Function<String, String> contextProvider;

    /**
     * Creates an instance of {@link EventProcessorResultListener} based on the {@link EventProcessorStatusRefresh}.
     *
     * @param refreshEventProcessorStatus      used to require a refresh of the status of the event processors.
     * @param eventProcessorIdentifierProvider used to retrieve the token store identifier for the specified processor
     *                                         name
     *                                         and client name
     */
    @Autowired
    public EventProcessorResultListener(EventProcessorStatusRefresh refreshEventProcessorStatus,
                                        EventProcessorIdentifierProvider eventProcessorIdentifierProvider,
                                        ClientContextProvider contextProvider) {
        this(refreshEventProcessorStatus::run, eventProcessorIdentifierProvider, contextProvider);
    }

    /**
     * Creates an instance of {@link EventProcessorResultListener} based on the specified refresh operation.
     *
     * @param refreshOperation                 used to require a refresh of the status of the event processors.
     * @param eventProcessorIdentifierProvider used to retrieve the token store identifier for the specified processor
     *                                         name
     *                                         and client name
     */
    public EventProcessorResultListener(BiConsumer<String, EventProcessorIdentifier> refreshOperation,
                                        BiFunction<String, String, EventProcessorIdentifier> eventProcessorIdentifierProvider,
                                        Function<String, String> contextProvider) {
        this.refreshOperation = refreshOperation;
        this.eventProcessorIdentifierProvider = eventProcessorIdentifierProvider;
        this.contextProvider = contextProvider;
    }

    /**
     * Refresh the state of the event processor after a merge operation has been performed.
     *
     * @param event the event describing the event processor that has been merged
     */
    @EventListener
    public void on(EventProcessorEvents.MergeSegmentsSucceeded event) {
        refresh(event.clientName(), event.processorName());
    }

    /**
     * Refresh the state of the event processor after a slit operation has been performed.
     *
     * @param event the event describing the event processor that has been split
     */
    @EventListener
    public void on(EventProcessorEvents.SplitSegmentsSucceeded event) {
        refresh(event.clientName(), event.processorName());
    }

    private void refresh(String clientName, String processorName) {
        EventProcessorIdentifier processor = eventProcessorIdentifierProvider.apply(clientName, processorName);
        String context = contextProvider.apply(clientName);
        refreshOperation.accept(context, processor);
    }
}
