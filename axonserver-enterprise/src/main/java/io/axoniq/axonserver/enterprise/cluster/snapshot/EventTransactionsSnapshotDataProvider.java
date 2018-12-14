package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.TransactionInformation;
import reactor.core.publisher.Flux;

/**
 * @author Milan Savic
 */
public class EventTransactionsSnapshotDataProvider implements SnapshotDataProvider {

    private static final String SNAPSHOT_TYPE = "eventsTransaction";

    private final String context;
    private final LocalEventStore localEventStore;

    public EventTransactionsSnapshotDataProvider(String context, LocalEventStore localEventStore) {
        this.context = context;
        this.localEventStore = localEventStore;
    }

    @Override
    public int order() {
        return 40;
    }

    @Override
    public Flux<SerializedObject> provide(long from, long to) {
        return Flux.fromIterable(() -> localEventStore.eventTransactionsIterator(context, from, to))
                   .map(transactionWithToken -> SerializedObject.newBuilder()
                                                                .setType(SNAPSHOT_TYPE)
                                                                .setData(transactionWithToken.toByteString())
                                                                .build());
    }

    @Override
    public boolean canConsume(String type) {
        return SNAPSHOT_TYPE.equals(type);
    }

    @Override
    public void consume(SerializedObject serializedObject) {
        try {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(serializedObject.getData());
            localEventStore.syncEvents(context, new TransactionInformation(transactionWithToken.getIndex()), transactionWithToken);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize events transaction.", e);
        }
    }
}
