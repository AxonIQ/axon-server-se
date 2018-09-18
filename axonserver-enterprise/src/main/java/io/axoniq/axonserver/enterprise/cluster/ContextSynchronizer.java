package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ContextAction;
import io.axoniq.axonserver.grpc.internal.ContextUpdate;
import io.axoniq.axonserver.grpc.internal.NodeRole;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Component
public class ContextSynchronizer {

    private final ClusterController clusterController;

    public ContextSynchronizer(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    @EventListener(condition = "!#a0.proxied")
    public void on(ContextEvents.ContextCreated contextCreated) {
        ContextUpdate update = ContextUpdate.newBuilder().setAction(ContextAction.MERGE_CONTEXT)
                                            .setName(contextCreated.getName())
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
                                            .build();

        clusterController.publish(ConnectorCommand.newBuilder().setContext(update).build());
    }

    @EventListener(condition = "!#a0.proxied")
    public void on(ContextEvents.NodeAddedToContext contextCreated) {
        ContextUpdate update = ContextUpdate.newBuilder().setAction(ContextAction.ADD_NODES)
                                            .setName(contextCreated.getName())
                                            .addNodes(NodeRole.newBuilder()
                                                                   .setName(contextCreated.getNode().getName())
                                                                   .setMessaging(contextCreated.getNode().isMessaging())
                                                                   .setStorage(contextCreated.getNode().isStorage())
                                                                   .build())
                                            .build();

        clusterController.publish(ConnectorCommand.newBuilder().setContext(update).build());
    }

    @EventListener(condition = "!#a0.proxied")
    public void on(ContextEvents.NodeDeletedFromContext contextCreated) {
        ContextUpdate update = ContextUpdate.newBuilder().setAction(ContextAction.DELETE_NODES)
                                            .setName(contextCreated.getName())
                                            .addNodes(NodeRole.newBuilder()
                                                              .setName(contextCreated.getNode())
                                                              .build())
                                            .build();

        clusterController.publish(ConnectorCommand.newBuilder().setContext(update).build());
    }
}
