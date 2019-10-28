package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.RaftAdminGroup;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * @since 4.1
 */
@Component
public class RaftGroupRepositoryManager {
    private final JpaRaftGroupNodeRepository raftGroupNodeRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;

    public RaftGroupRepositoryManager(
            JpaRaftGroupNodeRepository raftGroupNodeRepository, MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    public Set<JpaRaftGroupNode> getMyContexts() {
        return findByNodeName(messagingPlatformConfiguration.getName());
    }

    /**
     * @return the names of all raft groups that have an event store on this node
     */
    public Set<String> storageContexts() {
        return raftGroupNodeRepository.findByNodeName(messagingPlatformConfiguration.getName())
                                      .stream()
                                      .filter(group -> RoleUtils.hasStorage(group.getRole()))
                                      .map(JpaRaftGroupNode::getGroupId)
                                      .filter(n -> !RaftAdminGroup.isAdmin(n))
                                      .collect(Collectors.toSet());
    }

    public Set<JpaRaftGroupNode> findByGroupId(String groupId) {
        return raftGroupNodeRepository.findByGroupId(groupId);
    }

    public void delete(String groupId) {
        raftGroupNodeRepository.deleteAll(raftGroupNodeRepository.findByGroupId(groupId));

    }

    public void update(String groupId, List<Node> nodes) {
        delete(groupId);
        nodes.forEach(n -> {
            JpaRaftGroupNode jpaRaftGroupNode = new JpaRaftGroupNode(groupId, n);
            raftGroupNodeRepository.save(jpaRaftGroupNode);
        });
    }

    public Set<JpaRaftGroupNode> findByNodeName(String nodeName) {
        return new HashSet<>(raftGroupNodeRepository.findByNodeName(nodeName));
    }
}
