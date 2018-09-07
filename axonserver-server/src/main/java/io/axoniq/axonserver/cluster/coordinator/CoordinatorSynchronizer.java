package io.axoniq.axonserver.cluster.coordinator;

import io.axoniq.axonserver.ClusterEvents;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.NodeContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.function.Function;

/**
 * Created by Sara Pellegrini on 24/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class CoordinatorSynchronizer {

    private final String thisNodeName;

    private final Publisher<ConnectorCommand> clusterPublisher;

    private final Function<String, Iterable<String>> contextsByCoordinator;

    @Autowired
    public CoordinatorSynchronizer(@Value("${axoniq.axonserver.name:name}") String thisNodeName,
                                   Publisher<ConnectorCommand> clusterPublisher,
                                   AxonHubManager hubManager) {
        this(thisNodeName, clusterPublisher, hubManager::contextsCoordinatedBy);
    }

    public CoordinatorSynchronizer(String thisNodeName,
                                   Publisher<ConnectorCommand> clusterPublisher,
                                   Function<String, Iterable<String>> contextsByCoordinator) {
        this.thisNodeName = thisNodeName;
        this.clusterPublisher = clusterPublisher;
        this.contextsByCoordinator = contextsByCoordinator;
    }

    @EventListener(condition = "!#a0.forwarded")
    public void on(ClusterEvents.BecomeCoordinator event) {
        clusterPublisher.publish(ConnectorCommand.newBuilder().setCoordinatorConfirmation(
                NodeContext.newBuilder().setNodeName(event.node()).setContext(event.context())
        ).build());
    }

    @EventListener(condition = "!#a0.forwarded")
    public void on(ClusterEvents.CoordinatorStepDown event) {
        clusterPublisher.publish(ConnectorCommand.newBuilder().setCoordinatorConfirmation(
                NodeContext.newBuilder().setContext(event.context()).build()
        ).build());
    }

    @EventListener
    public void on(ClusterEvents.AxonHubInstanceConnected event) {
        contextsByCoordinator.apply(thisNodeName).forEach(context -> {
            event.getRemoteConnection().publish(ConnectorCommand.newBuilder().setCoordinatorConfirmation(
                    NodeContext.newBuilder().setContext(context).setNodeName(thisNodeName)
            ).build());
        });
    }
}
