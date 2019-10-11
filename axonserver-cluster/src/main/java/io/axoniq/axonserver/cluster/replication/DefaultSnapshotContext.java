package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;

/**
 * {@link SnapshotContext} implementation that takes the data boundaries from the {@link AppendEntryFailure}.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class DefaultSnapshotContext implements SnapshotContext {

    private final AppendEntryFailure failure;
    private final boolean eventStore;

    /**
     * Constructor
     *
     * @param failure    failure response from remote peer
     * @param eventStore true if remote peer is an eventStore
     */
    public DefaultSnapshotContext(AppendEntryFailure failure, boolean eventStore) {
        this.failure = failure;
        this.eventStore = eventStore;
    }

    @Override
    public long fromEventSequence() {
        if (!eventStore) {
            return Long.MAX_VALUE;
        }
        return failure.getLastAppliedEventSequence()+1;
    }

    @Override
    public long fromSnapshotSequence() {
        if (!eventStore) {
            return Long.MAX_VALUE;
        }
        return failure.getLastAppliedSnapshotSequence()+1;
    }

    @Override
    public String toString() {
        return "DefaultSnapshotContext[fromEvent=" + fromEventSequence() + ",fromSnapshot=" + fromSnapshotSequence() + "]";
    }
}
