package io.axoniq.axonserver.cluster.jpa;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.Set;

/**
 * Repository of raft group member assignments.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public interface ReplicationGroupMemberRepository
        extends JpaRepository<ReplicationGroupMember, ReplicationGroupMember.Key> {

    /**
     * Finds all members for a raft group
     *
     * @param groupId the raft group id
     * @return set of entries for the given group id
     */
    Set<ReplicationGroupMember> findByGroupId(String groupId);

    /**
     * Finds all raft groups where a node is member of
     * @param nodeName the name of the node
     * @return set of entries for the given node name
     */
    Set<ReplicationGroupMember> findByNodeName(String nodeName);

    /**
     * Finds a raft group member based on the groupId and the node name.
     *
     * @param groupId  the raft group id
     * @param nodeName the node name
     * @return optional group member
     */
    Optional<ReplicationGroupMember> findByGroupIdAndNodeName(String groupId, String nodeName);

    /**
     * Deletes all members from the raft group
     * @param groupId the raft group id
     */
    void deleteAllByGroupId(String groupId);
}
