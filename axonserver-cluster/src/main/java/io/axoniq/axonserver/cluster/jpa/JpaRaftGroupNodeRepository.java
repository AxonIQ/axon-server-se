package io.axoniq.axonserver.cluster.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Set;

/**
 * Author: marc
 */
public interface JpaRaftGroupNodeRepository extends JpaRepository<JpaRaftGroupNode, JpaRaftGroupNode.Key> {
    Set<JpaRaftGroupNode> findByGroupId(String groupId);

    Set<JpaRaftGroupNode> findByNodeName(String nodeName);
}
