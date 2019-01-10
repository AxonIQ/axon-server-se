package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.TransactionInformation;
import reactor.core.publisher.Flux;

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

    /**
     * Creates Event Transaction Snapshot Data Store for streaming/applying event transaction data.
     *
     * @param context         application context
     * @param localEventStore event store used retrieving/saving event transactions
     */
    public EventTransactionsSnapshotDataStore(String context, LocalEventStore localEventStore) {
        this.context = context;
        this.localEventStore = localEventStore;
    }

    @Override
    public int order() {
        return 40;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(long fromEventSequence, long toEventSequence) {
        return Flux.fromIterable(() -> localEventStore.eventTransactionsIterator(context, fromEventSequence,
                                                                                 toEventSequence))
                   .map(transactionWithToken -> SerializedObject.newBuilder()
                                                                .setType(SNAPSHOT_TYPE)
                                                                .setData(transactionWithToken.toByteString())
                                                                .build());
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return SNAPSHOT_TYPE.equals(type);
    }

    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(serializedObject.getData());
            localEventStore.syncEvents(context,
                                       new TransactionInformation(transactionWithToken.getIndex()),
                                       transactionWithToken);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize events transaction.", e);
        }
    }

    @Override
    public void clear() {
        // we don't delete events
    }
}
