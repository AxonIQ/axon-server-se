package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * Includes the nodes known on this (admin) node in the snapshot.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class AdminClusterSnapshotDataStore implements SnapshotDataStore {

    private static final Logger logger = LoggerFactory.getLogger(AdminClusterSnapshotDataStore.class);

    public static final String ENTRY_TYPE = NodeInfo.class.getName();
    private final String replicationGroup;
    private final ClusterController clusterController;
    private final boolean adminContext;


    public AdminClusterSnapshotDataStore(String replicationGroup, ClusterController clusterController) {
        this.replicationGroup = replicationGroup;
        this.clusterController = clusterController;
        this.adminContext = isAdmin(replicationGroup);
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (!adminContext) {
            return Flux.empty();
        }

        return Flux.fromIterable(clusterController.nodes().collect(Collectors.toList()))
                   .map(ClusterNode::toNodeInfo)
                   .map(this::toSerializedObject);
    }

    private SerializedObject toSerializedObject(NodeInfo nodeInfo) {
        return SerializedObject.newBuilder()
                               .setData(nodeInfo.toByteString())
                               .setType(ENTRY_TYPE)
                               .build();
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role)
            throws SnapshotDeserializationException {
        try {
            NodeInfo nodeInfo = NodeInfo.parseFrom(serializedObject.getData());
            logger.debug("{}: add nodeInfo {}", replicationGroup, nodeInfo);
            clusterController.addConnection(nodeInfo);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        // no-op, don't want to start by clearing nodes
    }
}
