package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

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
    private final boolean adminContext;

    /**
     * Creates Snapshot Transaction Snapshot Data Store for streaming/applying snapshot transaction data.
     *
     * @param context         application context
     * @param localEventStore event store used retrieving/saving snapshot transactions
     */
    public SnapshotTransactionsSnapshotDataStore(String context, LocalEventStore localEventStore) {
        this.context = context;
        this.localEventStore = localEventStore;
        this.adminContext = isAdmin(context);
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if( adminContext) return Flux.empty();
        long fromToken = installationContext.fromSnapshotSequence();
        long toToken = localEventStore.getLastSnapshot(context)+1;

        if( fromToken >= toToken) return Flux.empty();
        return Flux.fromIterable(() -> localEventStore.snapshotTransactionsIterator(context, fromToken, toToken))
                   .map(transactionWithToken -> SerializedObject.newBuilder()
                                                                .setType(SNAPSHOT_TYPE)
                                                                .setData(SerializedTransactionWithTokenConverter
                                                                                 .asByteString(transactionWithToken))
                                                                .build());
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return !adminContext && type.equals(SNAPSHOT_TYPE);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(serializedObject.getData());
            localEventStore.initContext(context, false);
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
