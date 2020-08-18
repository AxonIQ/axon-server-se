package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.StreamSupport.stream;

/**
 * Iterable of {@link Client}s that contain the processor defined by {@link EventProcessorIdentifier}
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public class ClientsByEventProcessor implements Clients {

    private final String context;

    private final Clients allClients;

    private final ClientProcessors eventProcessors;

    /**
     * Creates an instance of {@link ClientsByEventProcessor} based on the iterable of all registered {@link Clients}s,
     * the iterable of all registered {@link ClientProcessor}s, the specified context and the specified
     * {@link EventProcessorIdentifier}.
     *
     * @param processorId         the identifier of the event processor we are interested in
     * @param context             the context we are interested in
     * @param allClients          all {@link Client}s connected
     * @param allClientProcessors all the {@link ClientProcessor}s instances of connected clients
     */
    public ClientsByEventProcessor(EventProcessorIdentifier processorId,
                                   String context,
                                   Clients allClients,
                                   ClientProcessors allClientProcessors) {
        this.context = context;
        this.allClients = allClients;
        this.eventProcessors = new ClientProcessorsByIdentifier(allClientProcessors, context, processorId);
    }


    /**
     * Creates an instance of {@link ClientsByEventProcessor} based on the iterable of all registered {@link Clients}s,
     * the iterable of all registered {@link ClientProcessor}s belonging to the event processor we are interested in and
     * the specified context.
     *
     * @param context         the context we are interested in
     * @param allClients      all {@link Client}s connected
     * @param eventProcessors all the {@link ClientProcessor}s belonging to the event processor we are interested in
     */
    public ClientsByEventProcessor(String context,
                                   Clients allClients,
                                   ClientProcessors eventProcessors) {
        this.context = context;
        this.allClients = allClients;
        this.eventProcessors = eventProcessors;
    }

    @NotNull
    @Override
    public Iterator<Client> iterator() {
        List<String> clients = stream(eventProcessors.spliterator(), false)
                .map(ClientProcessor::clientName)
                .collect(Collectors.toList());

        return stream(allClients.spliterator(), false)
                .filter(client -> clients.contains(client.name()))
                .filter(client -> client.belongsToContext(context))
                .iterator();
    }
}
