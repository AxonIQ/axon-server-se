package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import reactor.core.publisher.Flux;

import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * Includes the nodes known on this (admin) node in the snapshot.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class ClusterSnapshotDataStore implements SnapshotDataStore {

    public static final String ENTRY_TYPE = NodeInfo.class.getName();
    private final ClusterController clusterController;
    private final boolean adminContext;


    public ClusterSnapshotDataStore(String context, ClusterController clusterController) {
        this.clusterController = clusterController;
        this.adminContext = isAdmin(context);
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
    public void applySnapshotData(SerializedObject serializedObject) throws SnapshotDeserializationException {
        try {
            NodeInfo nodeInfo = NodeInfo.parseFrom(serializedObject.getData());
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
