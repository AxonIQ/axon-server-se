package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

/**
 * Author: marc
 */
@Controller
public class EventStoreMasterSynchronizer {
    private final ClusterPublisher clusterPublisher;

    public EventStoreMasterSynchronizer(ClusterPublisher clusterPublisher) {
        this.clusterPublisher = clusterPublisher;
    }

    @EventListener
    public void on(ClusterEvents.BecomeMaster becomeMaster) {
        if( becomeMaster.isForwarded()) return;
        clusterPublisher.publish(ConnectorCommand.newBuilder()
                                                 .setMasterConfirmation(
                                                         NodeContextInfo.newBuilder()
                                                                        .setNodeName(becomeMaster.getNode())
                                                                        .setContext(becomeMaster.getContext())
                                                                        .build())
                                                 .build());
    }

    @EventListener
    public void on(ClusterEvents.MasterStepDown masterStepDown) {
        if( masterStepDown.isForwarded()) return;
        clusterPublisher.publish(ConnectorCommand.newBuilder()
                                                 .setMasterConfirmation(
                                                         NodeContextInfo.newBuilder()
                                                                        .setContext(masterStepDown.getContextName())
                                                                        .build())
                                                 .build());
    }
}
