package io.axoniq.axonserver.enterprise.topology;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 */
public class ClusterTopology implements Topology {
    private final ClusterController clusterController;
    private final GrpcRaftController raftController;

    public ClusterTopology(ClusterController clusterController, GrpcRaftController raftController) {
        this.clusterController = clusterController;
        this.raftController = raftController;
    }

    @Override
    public String getName() {
        return clusterController.getName();
    }

    @Override
    public boolean isMultiContext() {
        return true;
    }

    @Override
    public boolean isActive(AxonServerNode node) {
        return clusterController.isActive(node.getName());
    }

    @Override
    public Stream<? extends AxonServerNode> nodes() {
        if( clusterController.isAdminNode()) {
            return clusterController.nodes();
        }

        return raftController.nodes();
    }

    @Override
    public List<AxonServerNode> getRemoteConnections() {
        return clusterController.getRemoteConnections().stream().map(RemoteConnection::getClusterNode).collect(Collectors.toList());
    }

    @Override
    public AxonServerNode getMe() {
        return clusterController.getMe();
    }

    @Override
    public AxonServerNode findNodeForClient(String clientName, String componentName, String context) {
        return clusterController.findNodeForClient(clientName, componentName, context);
    }

    @Override
    public Iterable<String> getMyContextNames() {
        return raftController.getMyContexts();
    }

    @Override
    public Iterable<String> getMyStorageContextNames() {
        Set<String> names = new HashSet<>();
        raftController.getMyContexts().forEach(c -> {
            if (!isAdmin(c)) {
                names.add(c);
            }
        });
        Executors.defaultThreadFactory();
        return names;
    }


    @Override
    public boolean isAdminNode() {
        return clusterController.isAdminNode();
    }
}
