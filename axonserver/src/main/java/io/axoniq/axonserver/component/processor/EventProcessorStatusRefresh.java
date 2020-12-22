package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdated;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Responsible to refresh the event processor status for a specific event processor.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class EventProcessorStatusRefresh {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessorStatusRefresh.class);

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    private final Duration timeout;

    private final ClientProcessors all;

    private final ApplicationEventPublisher eventPublisher;

    private final List<Consumer<EventProcessorStatusUpdated>> updateListeners = new CopyOnWriteArrayList<>();

    /**
     * Creates an instance of {@link EventProcessorStatusRefresh} based on all register event processor and the
     * {@link ApplicationEventPublisher}. The timeout waited for the operation to be completed is set to the default
     * value of 10 seconds.
     *
     * @param allClientProcessors used to get the connected clients with a specific event processor
     * @param eventPublisher      used to publish internal events
     */
    @Autowired
    public EventProcessorStatusRefresh(ClientProcessors allClientProcessors,
                                       ApplicationEventPublisher eventPublisher) {
        this(DEFAULT_TIMEOUT, allClientProcessors, eventPublisher);
    }

    /**
     * Creates an instance of {@link EventProcessorStatusRefresh} based on the specified parameters.
     *
     * @param timeout        the time waited in order to receive updates from clients before to complete exceptionally
     *                       the refresh operation
     * @param all            used to get the connected clients with a specific event processor
     * @param eventPublisher used to publish internal events
     */
    public EventProcessorStatusRefresh(Duration timeout,
                                       ClientProcessors all,
                                       ApplicationEventPublisher eventPublisher) {
        this.timeout = timeout;
        this.all = all;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Requests the updated event processor status. Returns a {@link CompletableFuture} that completes as soon as the
     * updated status as been received from all the connected clients that declared the specified event processor.
     *
     * @param processorId the identifier of the processor for which an update is requested
     * @return a {@link CompletableFuture} that completes as soon as the status has been updated
     */
    public CompletableFuture<Void> run(String context, EventProcessorIdentifier processorId) {
        return CompletableFuture.runAsync(() -> {

            ClientProcessorsByIdentifier matchingClients = new ClientProcessorsByIdentifier(all, context, processorId);
            Set<String> clientIds = StreamSupport.stream(matchingClients.spliterator(), false)
                                                 .map(ClientProcessor::clientId)
                                                 .collect(Collectors.toSet());
            CountDownLatch clientProcessorStatusUpdateLatch = new CountDownLatch(clientIds.size());

            Consumer<EventProcessorStatusUpdated> statusUpdateListener = statusEvent -> {
                String clientId = statusEvent.eventProcessorStatus().getClientId();
                String processorName = statusEvent.eventProcessorStatus().getEventProcessorInfo().getProcessorName();
                if (clientIds.contains(clientId) && processorName.equals(processorId.name())) {
                    clientIds.remove(clientId);
                    clientProcessorStatusUpdateLatch.countDown();
                }
            };

            updateListeners.add(statusUpdateListener);
            matchingClients.forEach(client -> eventPublisher.publishEvent(
                    new EventProcessorEvents.ProcessorStatusRequest(context,
                                                                    client.clientId(), processorId.name(), false)
            ));
            try {
                boolean updated = clientProcessorStatusUpdateLatch.await(timeout.toMillis(), MILLISECONDS);
                if (!updated) {
                    throw new RuntimeException(
                            "Event processor status update has not be completed in 10 seconds. No response from "
                                    + clientIds);
                }
            } catch (InterruptedException e) {
                LOGGER.warn("The refresh of the event processors status was interrupted. ", e);
                Thread.currentThread().interrupt();
            } finally {
                updateListeners.remove(statusUpdateListener);
            }
        });
    }

    /**
     * Event handler towards the {@link EventProcessorStatusUpdated}, used to verify if all the status updates have been
     * received.
     *
     * @param event the {@link EventProcessorStatusUpdated} signaling the update of Event Processor Status
     */
    @EventListener
    public void on(EventProcessorStatusUpdated event) {
        updateListeners.forEach(listener -> listener.accept(event));
    }
}
