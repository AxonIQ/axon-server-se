package io.axoniq.axonserver.enterprise.replication.logconsumer;


import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import org.springframework.stereotype.Component;

/**
 * Applies Node Configuration to ensure that nodes that are registered without any context are still added to all admin
 * nodes.
 * Runs in Admin context only.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class AdminNodeConsumer implements LogEntryConsumer {

    private final ClusterController clusterController;

    public AdminNodeConsumer(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    @Override
    public String entryType() {
        return AdminNodeConsumer.class.getName();
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        NodeInfo nodeInfo = NodeInfo.parseFrom(e.getSerializedObject().getData());
        clusterController.addConnection(nodeInfo);
    }
}
