package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * Snapshot data store for event transaction data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class EventTransactionsSnapshotDataStore implements SnapshotDataStore {

    private static final String SNAPSHOT_TYPE = "eventsTransaction";

    private final String context;
    private final LocalEventStore localEventStore;
    private final boolean adminContext;

    /**
     * Creates Event Transaction Snapshot Data Store for streaming/applying event transaction data.
     *
     * @param context         application context
     * @param localEventStore event store used retrieving/saving event transactions
     */
    public EventTransactionsSnapshotDataStore(String context, LocalEventStore localEventStore) {
        this.context = context;
        this.localEventStore = localEventStore;
        this.adminContext = isAdmin(context);
    }

    @Override
    public int order() {
        return 40;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if( adminContext) return Flux.empty();
        long fromToken = installationContext.fromEventSequence();
        long toToken = localEventStore.getLastToken(context) + 1;

        if( fromToken >= toToken) return Flux.empty();
        return Flux.fromIterable(() -> localEventStore.eventTransactionsIterator(context, fromToken, toToken))
                   .map(transactionWithToken -> SerializedObject.newBuilder()
                                                                .setType(SNAPSHOT_TYPE)
                                                                .setData(SerializedTransactionWithTokenConverter.asByteString(transactionWithToken))
                                                                .build());
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return !adminContext && SNAPSHOT_TYPE.equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(serializedObject.getData());
            localEventStore.syncEvents(context, SerializedTransactionWithTokenConverter.asSerializedTransactionWithToken(transactionWithToken));
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize events transaction.", e);
        }
    }

    @Override
    public void clear() {
        // we don't delete events
    }
}
