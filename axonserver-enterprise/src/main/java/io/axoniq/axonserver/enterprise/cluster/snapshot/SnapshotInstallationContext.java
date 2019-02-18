package io.axoniq.axonserver.enterprise.cluster.snapshot;

/**
 * The information related to a specific Raft snapshot installation.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public interface SnapshotInstallationContext {

    /**
     * Gets the first Event's sequence that should be transmitted within the raft snapshot.
     * @return lower boundary (inclusive) in terms of Event's sequence of snapshot data
     */
    long fromEventSequence();

    /**
     * Gets the first Snapshot's sequence that should be transmitted within the raft snapshot.
     * @return lower boundary (inclusive) in terms of Snapshot's sequence of snapshot data
     */
    long fromSnapshotSequence();

}
