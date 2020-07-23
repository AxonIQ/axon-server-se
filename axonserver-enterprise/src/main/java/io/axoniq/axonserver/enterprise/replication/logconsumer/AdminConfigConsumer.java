package io.axoniq.axonserver.enterprise.replication.logconsumer;


import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Applies replication group configuration in Admin replication group only.
 *
 * @author Marc Gathier
 */
@Component
public class AdminConfigConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(AdminConfigConsumer.class);

    private final AdminReplicationGroupController adminReplicationGroupController;

    public AdminConfigConsumer(AdminReplicationGroupController adminReplicationGroupController) {
        this.adminReplicationGroupController = adminReplicationGroupController;
    }

    @Override
    public String entryType() {
        return ReplicationGroupConfiguration.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        ReplicationGroupConfiguration replicationGroupConfiguration = ReplicationGroupConfiguration
                .parseFrom(e.getSerializedObject().getData());
        logger.debug("{}: received data: {}", groupId, replicationGroupConfiguration);
        adminReplicationGroupController.updateReplicationGroup(replicationGroupConfiguration);
    }
}
