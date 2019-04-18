package io.axoniq.axonserver.cluster.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Set;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public interface JpaRaftGroupNodeRepository extends JpaRepository<JpaRaftGroupNode, JpaRaftGroupNode.Key> {
    Set<JpaRaftGroupNode> findByGroupId(String groupId);

    Set<JpaRaftGroupNode> findByNodeName(String nodeName);
}
