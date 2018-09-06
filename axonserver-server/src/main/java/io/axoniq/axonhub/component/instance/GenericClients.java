package io.axoniq.axonhub.component.instance;

import io.axoniq.axonhub.ClusterEvents;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
@Primary @Component
public class GenericClients implements Clients {

    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Map<String, Client> clientRegistrations = new HashMap<>();

    public GenericClients(MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public Iterator<Client> iterator() {
        return clientRegistrations.values().iterator();
    }

    @EventListener
    public void on(ClusterEvents.ApplicationDisconnected event) {
        this.clientRegistrations.remove(event.getClient());
    }

    @EventListener
    public void on(ClusterEvents.ApplicationConnected event) {
        this.clientRegistrations.put(event.getClient(),
                                     new GenericClient(event.getClient(),
                                                       event.getComponentName(),
                                                       event.getContext(),
                                                       event.isProxied() ? event.getProxy() : messagingPlatformConfiguration
                                                               .getName()));
    }
}
