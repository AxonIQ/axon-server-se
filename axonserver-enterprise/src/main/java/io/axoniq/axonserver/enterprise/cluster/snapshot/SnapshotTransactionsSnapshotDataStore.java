package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import reactor.core.publisher.Flux;

/**
 * Snapshot data store for snapshot transactions data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class SnapshotTransactionsSnapshotDataStore implements SnapshotDataStore {

    private static final String SNAPSHOT_TYPE = "snapshotsTransaction";

    private final String context;
    private final LocalEventStore localEventStore;

    /**
     * Creates Snapshot Transaction Snapshot Data Store for streaming/applying snapshot transaction data.
     *
     * @param context         application context
     * @param localEventStore event store used retrieving/saving snapshot transactions
     */
    public SnapshotTransactionsSnapshotDataStore(String context, LocalEventStore localEventStore) {
        this.context = context;
        this.localEventStore = localEventStore;
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(long fromEventSequence, long toEventSequence) {
        return Flux.fromIterable(() -> localEventStore.snapshotTransactionsIterator(context, fromEventSequence,
                                                                                    toEventSequence))
                   .map(transactionWithToken -> SerializedObject.newBuilder()
                                                                .setType(SNAPSHOT_TYPE)
                                                                .setData(SerializedTransactionWithTokenConverter
                                                                                 .asByteString(transactionWithToken))
                                                                .build());
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return type.equals(SNAPSHOT_TYPE);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(serializedObject.getData());
            localEventStore.syncSnapshots(context, SerializedTransactionWithTokenConverter.asSerializedTransactionWithToken(transactionWithToken));
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize events transaction.", e);
        }
    }

    @Override
    public void clear() {
        // we don't delete snapshots
    }
}
