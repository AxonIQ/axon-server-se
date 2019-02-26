package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Handles a new configuration log entry. It checks if the node has a connection to all nodes in the configuration, and if not
 * sets up the connections.
 *
 * @author Marc Gathier
 */
@Component
public class NewConfigurationConsumer implements LogEntryConsumer {
    private final Logger logger = LoggerFactory.getLogger(NewConfigurationConsumer.class);
    private final ClusterController clusterController;

    public NewConfigurationConsumer(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if( e.hasNewConfiguration()) {
            Config configuration = e.getNewConfiguration();
            logger.debug("{}: received config: {}", groupId, configuration);
            configuration.getNodesList().forEach(clusterController::connect);
        }
    }
}
