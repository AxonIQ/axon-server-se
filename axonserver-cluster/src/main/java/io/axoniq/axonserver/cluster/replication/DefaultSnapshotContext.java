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

    public DefaultSnapshotContext(AppendEntryFailure failure) {
        this.failure = failure;
    }

    @Override
    public long fromEventSequence() {
        return failure.getLastAppliedEventSequence()+1;
    }

    @Override
    public long fromSnapshotSequence() {
        return failure.getLastAppliedSnapshotSequence()+1;
    }
}
