package io.axoniq.axonserver.enterprise.cluster.coordinator;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.NodeContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 24/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ConfirmationSource {

    private final Supplier<String> thisNodeName;

    private final Publisher<ConnectorCommand> clusterPublisher;

    private final Function<String,String> coordinatorForContext;

    @Autowired
    public ConfirmationSource(MessagingPlatformConfiguration messagingPlatformConfiguration,
                              Publisher<ConnectorCommand> clusterPublisher,
                              AxonHubManager axonHubManager) {
        this(messagingPlatformConfiguration::getName,
             clusterPublisher,
             axonHubManager::coordinatorFor);
    }

    ConfirmationSource(Supplier<String> thisNodeName,
                       Publisher<ConnectorCommand> clusterPublisher,
                       Function<String,String> coordinatorForContext) {
        this.thisNodeName = thisNodeName;
        this.clusterPublisher = clusterPublisher;
        this.coordinatorForContext = coordinatorForContext;
    }

    @EventListener
    public void on(RequestToBeCoordinatorReceived event) {
        String context = event.request().getContext();
        String coordinator = coordinatorForContext.apply(context);
        if (coordinator != null) {
            clusterPublisher.publish(ConnectorCommand.newBuilder().setCoordinatorConfirmation(
                    NodeContext.newBuilder().setContext(context).setNodeName(coordinator)
            ).build());
            event.callback().accept(false);
            return;
        }
        event.callback().accept(event.request().getNodeName().compareTo(thisNodeName.get()) < 0);
    }
}
