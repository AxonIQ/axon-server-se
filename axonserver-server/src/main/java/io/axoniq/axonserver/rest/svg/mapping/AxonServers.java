package io.axoniq.axonserver.rest.svg.mapping;

import io.axoniq.axonserver.cluster.ClusterController;
import io.axoniq.axonserver.cluster.jpa.ClusterNode;
import io.axoniq.axonserver.context.jpa.Context;
import io.axoniq.axonserver.message.event.EventStoreManager;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class AxonServers implements Iterable<AxonServer> {

    private final ClusterController clusterController;
    private final EventStoreManager eventStoreManager   ;

    public AxonServers(ClusterController clusterController,
                       EventStoreManager eventStoreManager) {
        this.clusterController = clusterController;
        this.eventStoreManager = eventStoreManager;
    }

    @Override
    @Nonnull
    public Iterator<AxonServer> iterator() {
        return  clusterController.messagingNodes()
                                 .sorted(Comparator.comparing(ClusterNode::getName))
                                 .map(node -> (AxonServer) new AxonServer() {

                                     @Override
                                     public boolean isActive() {
                                         return clusterController.isActive(node);
                                     }

                                     @Override
                                     public ClusterNode node() {
                                         return node;
                                     }

                                     @Override
                                     public List<String> contexts() {
                                         return node.getMessagingContexts().stream().map(Context::getName).sorted().collect(
                                                 Collectors.toList());
                                     }

                                     @Override
                                     public List<AxonDB> storage() {
                                         return node.getStorageContexts().stream().map(context -> new AxonDB() {
                                             @Override
                                             public String context() {
                                                 return context.getName();
                                             }

                                             @Override
                                             public boolean master() {
                                                 return eventStoreManager.isMaster(node.getName(), context.getName());
                                             }
                                         } ).sorted(Comparator.comparing(a -> a.context())).collect(Collectors.toList());
                                     }
                                 }).iterator();
    }
}
