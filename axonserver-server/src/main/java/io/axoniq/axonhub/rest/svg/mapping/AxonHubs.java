package io.axoniq.axonhub.rest.svg.mapping;

import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.context.jpa.Context;
import io.axoniq.axonhub.message.event.EventStoreManager;
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
public class AxonHubs implements Iterable<AxonHub> {

    private final ClusterController clusterController;
    private final EventStoreManager eventStoreManager   ;

    public AxonHubs(ClusterController clusterController,
                    EventStoreManager eventStoreManager) {
        this.clusterController = clusterController;
        this.eventStoreManager = eventStoreManager;
    }

    @Override
    @Nonnull
    public Iterator<AxonHub> iterator() {
        return  clusterController.messagingNodes()
                                 .sorted(Comparator.comparing(ClusterNode::getName))
                                 .map(node -> (AxonHub) new AxonHub() {

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
