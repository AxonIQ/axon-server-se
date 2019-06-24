package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.cluster.NewConfigurationConsumer;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.grpc.cluster.Config;
import org.springframework.stereotype.Component;

/**
 * Handles a new configuration. It checks if the node has a connection to all nodes in the configuration, and if not
 * sets up the connections.
 *
 * @author Marc Gathier
 */
@Component
public class NewConfigurationConsumerImpl implements NewConfigurationConsumer {

    private final ClusterController clusterController;

    public NewConfigurationConsumerImpl(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    @Override
    public void consume(Config configuration) {
        configuration.getNodesList().forEach(clusterController::connect);
    }
}
