package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ContextAction;
import io.axoniq.axonserver.grpc.internal.ContextUpdate;
import io.axoniq.axonserver.grpc.internal.NodeRole;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Component
public class ContextSynchronizer {

    private final ClusterController clusterController;
    private final ModelVersionController modelVersionController;

    public ContextSynchronizer(ClusterController clusterController,
                               ModelVersionController modelVersionController) {
        this.clusterController = clusterController;
        this.modelVersionController = modelVersionController;
    }

    @EventListener(condition = "!#a0.proxied")
    public void on(ContextEvents.ContextCreated contextCreated) {
        ContextUpdate update = ContextUpdate.newBuilder().setAction(ContextAction.MERGE_CONTEXT)
                                            .setName(contextCreated.getName())
                                            .setGeneration(modelVersionController.incrementModelVersion(ClusterNode.class))
                                            .addAllNodes(contextCreated.getNodes().stream().map(r -> NodeRole.newBuilder()
                                                                                                             .setName(r.getName())
                                                                                                             .setMessaging(r.isMessaging())
                                                                                                             .setStorage(r.isStorage())
                                                                                                             .build()).collect(Collectors.toList()))
                                            .build();

        clusterController.publish(ConnectorCommand.newBuilder().setContext(update).build());
    }

    @EventListener(condition = "!#a0.proxied")
    public void on(ContextEvents.ContextDeleted contextCreated) {
        ContextUpdate update = ContextUpdate.newBuilder().setAction(ContextAction.DELETE_CONTEXT)
                                            .setName(contextCreated.getName())
                                            .setGeneration(modelVersionController.incrementModelVersion(ClusterNode.class))
                                            .build();

        clusterController.publish(ConnectorCommand.newBuilder().setContext(update).build());
    }

    @EventListener(condition = "!#a0.proxied")
    public void on(ContextEvents.NodeRolesUpdated contextCreated) {
        ContextUpdate update = ContextUpdate.newBuilder().setAction(ContextAction.NODES_UPDATED)
                                            .setName(contextCreated.getName())
                                            .setGeneration(modelVersionController.incrementModelVersion(ClusterNode.class))
                                            .addNodes(NodeRole.newBuilder()
                                                                   .setName(contextCreated.getNode().getName())
                                                                   .setMessaging(contextCreated.getNode().isMessaging())
                                                                   .setStorage(contextCreated.getNode().isStorage())
                                                                   .build())
                                            .build();

        clusterController.publish(ConnectorCommand.newBuilder().setContext(update).build());
    }
}
