package io.axoniq.axonserver.cluster.snapshot;

import io.axoniq.axonserver.grpc.cluster.Role;

/**
 * The information related to a specific Raft snapshot installation.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public interface SnapshotContext {

    /**
     * Gets the first Event's sequence that should be transmitted within the raft snapshot.
     *
     * @return lower boundary (inclusive) in terms of Event's sequence of snapshot data
     */
    default long fromEventSequence(String context) {
        return 0;
    }

    /**
     * Gets the first Snapshot's sequence that should be transmitted within the raft snapshot.
     *
     * @return lower boundary (inclusive) in terms of Snapshot's sequence of snapshot data
     */
    default long fromSnapshotSequence(String context) {
        return 0;
    }

    /**
     * Returns true of the remote peer supports replication groups.
     *
     * @return true of the remote peer supports replication groups
     */
    boolean supportsReplicationGroups();

    /**
     * Returns the role of the remote peer in the replication group.
     *
     * @return the role of the remote peer in the replication group
     */
    Role role();
}
