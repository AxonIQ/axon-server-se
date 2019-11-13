package io.axoniq.axonserver.enterprise.topology;

import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.NodeSelector;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 */
public class ClusterTopology implements Topology {
    private final ClusterController clusterController;
    private final GrpcRaftController raftController;
    private final NodeSelector nodeSelector;

    public ClusterTopology(ClusterController clusterController,
                           GrpcRaftController raftController,
                           NodeSelector nodeSelector) {
        this.clusterController = clusterController;
        this.raftController = raftController;
        this.nodeSelector = nodeSelector;
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

        return nodesFromRaftGroups();
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
        return nodeSelector.findNodeForClient(clientName, componentName, context);
    }

    @Override
    public Iterable<String> getMyContextNames() {
        return raftController.getContexts();
    }

    @Override
    public Iterable<String> getMyStorageContextNames() {
        Set<String> names = new HashSet<>();
        raftController.getStorageContexts().forEach(c -> {
            if (!isAdmin(c)) {
                names.add(c);
            }
        });
        return names;
    }


    @Override
    public boolean isAdminNode() {
        return clusterController.isAdminNode();
    }

    @Override
    public boolean validContext(String context) {
        if (isAdminNode()) {
            return true;
        }
        return raftController.getRaftGroup(context) != null;
    }

    public Stream<AxonServerNode> nodesFromRaftGroups() {
        Map<ClusterNode, Set<String>> contextPerNode = new HashMap<>();
        Map<ClusterNode, Set<String>> storageContextPerNode = new HashMap<>();
        clusterController.nodes().forEach(n -> contextPerNode.put(n, new HashSet<>()));
        raftController.getContexts().forEach(context -> {
            raftController.getRaftGroup(context).raftConfiguration().groupMembers().forEach(
                    node -> {
                        ClusterNode clusterNode = clusterController.getNode(node.getNodeName());
                        contextPerNode.computeIfAbsent(clusterNode, c -> new HashSet<>()).add(context);
                        if (RoleUtils.hasStorage(node.getRole())) {
                            storageContextPerNode.computeIfAbsent(clusterNode, c -> new HashSet<>()).add(context);
                        }
                    }
            );
        });
        return contextPerNode.entrySet().stream().map(e -> new AxonServerNode() {
            @Override
            public String getHostName() {
                return e.getKey().getHostName();
            }

            @Override
            public Integer getGrpcPort() {
                return e.getKey().getGrpcPort();
            }

            @Override
            public String getInternalHostName() {
                return e.getKey().getInternalHostName();
            }

            @Override
            public Integer getGrpcInternalPort() {
                return e.getKey().getGrpcInternalPort();
            }

            @Override
            public Integer getHttpPort() {
                return e.getKey().getHttpPort();
            }

            @Override
            public String getName() {
                return e.getKey().getName();
            }

            @Override
            public Collection<String> getContextNames() {
                return e.getValue();
            }

            @Override
            public Collection<String> getStorageContextNames() {
                return storageContextPerNode.getOrDefault(e.getKey(), Collections.emptySet());
            }
        });
    }

}
