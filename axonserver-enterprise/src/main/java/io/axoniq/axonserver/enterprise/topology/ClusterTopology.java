package io.axoniq.axonserver.enterprise.topology;

import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.NodeSelector;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContextRepository;
import io.axoniq.axonserver.enterprise.replication.ContextLeaderProvider;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
 */
@Component
public class ClusterTopology implements Topology {
    private final ClusterController clusterController;
    private final GrpcRaftController raftController;
    private final NodeSelector nodeSelector;
    private final ContextLeaderProvider contextLeaderProvider;
    private final ReplicationGroupContextRepository replicationGroupContextRepository;

    public ClusterTopology(ClusterController clusterController,
                           GrpcRaftController raftController,
                           NodeSelector nodeSelector,
                           ContextLeaderProvider contextLeaderProvider,
                           ReplicationGroupContextRepository replicationGroupContextRepository) {
        this.clusterController = clusterController;
        this.raftController = raftController;
        this.nodeSelector = nodeSelector;
        this.contextLeaderProvider = contextLeaderProvider;
        this.replicationGroupContextRepository = replicationGroupContextRepository;
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
    public boolean isLeader(String nodeName, String contextName) {
        return nodeName.equals(contextLeaderProvider.getLeaderOrWait(contextName, false));
    }

    @Override
    public Stream<? extends AxonServerNode> nodes() {
        if (clusterController.isAdminNode()) {
            return clusterController.nodes();
        }

        return nodesFromRaftGroups();
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
        return raftController.getAllNonAdminContexts();
    }

    @Override
    public Iterable<String> getMyStorageContextNames() {
        // TODO: check usages and change to getMyContextNames where applicable
        return raftController.getAllNonAdminContexts();
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
        raftController.getRaftGroups().forEach(replicationGroup -> {
            for (Node node : raftController.getRaftGroup(replicationGroup).raftConfiguration().groupMembers()) {
                ClusterNode clusterNode = clusterController.getNode(node.getNodeName());
                List<ReplicationGroupContext> contexts = replicationGroupContextRepository
                        .findByReplicationGroupName(replicationGroup);
                contexts.forEach(context -> {
                    contextPerNode.computeIfAbsent(clusterNode, c -> new HashSet<>()).add(context.getName());
                    if (RoleUtils.hasStorage(node.getRole())) {
                        storageContextPerNode.computeIfAbsent(clusterNode, c -> new HashSet<>()).add(context.getName());
                    }
                });
            }
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
