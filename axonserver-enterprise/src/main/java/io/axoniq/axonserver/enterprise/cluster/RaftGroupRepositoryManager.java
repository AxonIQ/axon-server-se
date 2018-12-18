package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.springframework.stereotype.Component;

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

    public Set<String> getMyContexts() {
        return raftGroupNodeRepository.findByNodeId(messagingPlatformConfiguration.getName())
                                      .stream()
                                      .map(n -> n.getGroupId())
                                      .collect(Collectors.toSet());
    }

    public Set<JpaRaftGroupNode> findByGroupId(String groupId) {
        return raftGroupNodeRepository.findByGroupId(groupId);
    }
}
