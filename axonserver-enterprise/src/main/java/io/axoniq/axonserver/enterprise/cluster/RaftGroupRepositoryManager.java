package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Author: marc
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

    public JpaRaftGroupNode initRaftGroup(String groupId) {
        JpaRaftGroupNode jpaRaftGroupNode = new JpaRaftGroupNode();
        jpaRaftGroupNode.setGroupId(groupId);
        jpaRaftGroupNode.setNodeId(messagingPlatformConfiguration.getName());
        jpaRaftGroupNode.setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname());
        jpaRaftGroupNode.setPort(messagingPlatformConfiguration.getInternalPort());
        return raftGroupNodeRepository.save(jpaRaftGroupNode);
    }

    public Set<JpaRaftGroupNode> getMyContexts() {
        return raftGroupNodeRepository.findByHostAndPort(messagingPlatformConfiguration.getFullyQualifiedInternalHostname(),
                                                         messagingPlatformConfiguration.getInternalPort())
                                      .stream()
                                      .collect(Collectors.toSet());
    }

    public Set<String> getMyContextNames() {
        return raftGroupNodeRepository.findByHostAndPort(messagingPlatformConfiguration.getFullyQualifiedInternalHostname(),
                                                         messagingPlatformConfiguration.getInternalPort())
                                      .stream()
                                      .map(JpaRaftGroupNode::getGroupId)
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
}
