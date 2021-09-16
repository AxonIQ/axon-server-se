package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.balancing.SameProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;

import java.util.Iterator;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

import static java.util.stream.StreamSupport.stream;

/**
 * Iterable of {@link ClientProcessor}s that have the same {@link EventProcessorIdentifier}
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public class ClientProcessorsByIdentifier implements ClientProcessors {

    private final ClientProcessors allClientProcessors;

    private final Predicate<ClientProcessor> sameEventProcessor;

    /**
     * Creates an instance of {@link ClientProcessorsByIdentifier} based on the specified iterable of all registered
     * {@link ClientProcessor}s and on the specified {@link EventProcessorIdentifier}.
     *
     * @param allClientProcessors      all the {@link ClientProcessor}s received from connected clients
     * @param eventProcessorIdentifier the identifier of the event processor we are interested in.
     */
    public ClientProcessorsByIdentifier(
            ClientProcessors allClientProcessors,
            EventProcessorIdentifier eventProcessorIdentifier) {
        this(allClientProcessors, new SameProcessor(eventProcessorIdentifier));
    }

    /**
     * Creates an instance of {@link ClientProcessorsByIdentifier} based on the specified iterable of all registered
     * {@link ClientProcessor}s and on the specified {@link Predicate} to identify the instances belonging to the
     * correct event processor.
     *
     * @param allClientProcessors all the {@link ClientProcessor}s received from connected clients
     * @param sameEventProcessor  the predicate to identify instances belonging to the event processor we are interested
     *                            in.
     */
    public ClientProcessorsByIdentifier(
            ClientProcessors allClientProcessors,
            Predicate<ClientProcessor> sameEventProcessor) {
        this.allClientProcessors = allClientProcessors;
        this.sameEventProcessor = sameEventProcessor;
    }

    @Nonnull
    @Override
    public Iterator<ClientProcessor> iterator() {
        return stream(allClientProcessors.spliterator(), false)
                .filter(sameEventProcessor)
                .iterator();
    }
}
