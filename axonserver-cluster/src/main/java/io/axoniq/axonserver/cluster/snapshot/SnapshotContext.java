package io.axoniq.axonserver.cluster.snapshot;

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
    default long fromEventSequence(){
        return 0;
    }

    /**
     * Gets the first Snapshot's sequence that should be transmitted within the raft snapshot.
     *
     * @return lower boundary (inclusive) in terms of Snapshot's sequence of snapshot data
     */
    default long fromSnapshotSequence(){
        return 0;
    }

}
