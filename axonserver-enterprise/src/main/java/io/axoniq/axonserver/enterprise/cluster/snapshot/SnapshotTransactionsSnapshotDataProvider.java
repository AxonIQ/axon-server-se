package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import reactor.core.publisher.Flux;

/**
 * @author Milan Savic
 */
public class SnapshotTransactionsSnapshotDataProvider implements SnapshotDataProvider {

    private static final String SNAPSHOT_TYPE = "snapshotsTransaction";

    private final String context;
    private final LocalEventStore localEventStore;

    public SnapshotTransactionsSnapshotDataProvider(String context, LocalEventStore localEventStore) {
        this.context = context;
        this.localEventStore = localEventStore;
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Flux<SerializedObject> provide(long from, long to) {
        return Flux.fromIterable(() -> localEventStore.snapshotTransactionsIterator(context, from, to))
                   .map(transactionWithToken -> SerializedObject.newBuilder()
                                                                .setType(SNAPSHOT_TYPE)
                                                                .setData(transactionWithToken.toByteString())
                                                                .build());
    }

    @Override
    public boolean canConsume(String type) {
        return type.equals(SNAPSHOT_TYPE);
    }

    @Override
    public void consume(SerializedObject serializedObject) {
        try {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(serializedObject.getData());
            localEventStore.syncSnapshots(context, transactionWithToken);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize events transaction.", e);
        }
    }
}
