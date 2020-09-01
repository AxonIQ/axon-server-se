package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.annotation.Nonnull;

/**
 * {@link ClientIdentifications} of all Axon Framework clients connected with the platform stream.
 *
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
@Component @Primary
public class PlatformConnectedClients implements ClientIdentifications {

    private final Set<ClientStreamIdentification> clientIdentifications = new CopyOnWriteArraySet<>();

    @Nonnull
    @Override
    public Iterator<ClientStreamIdentification> iterator() {
        return clientIdentifications.iterator();
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected event) {
        clientIdentifications.add(event.clientStreamIdentification());
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        clientIdentifications.remove(event.clientIdentification());
    }
}
