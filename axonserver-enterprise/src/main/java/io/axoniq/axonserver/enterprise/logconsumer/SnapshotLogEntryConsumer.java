package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter.asSerializedTransactionWithToken;

/**
 * Appends snapshot log entries.
 *
 * @author Milan Savic
 */
@Component
public class SnapshotLogEntryConsumer implements LogEntryConsumer {

    private static final String APPEND_SNAPSHOT = "Append.SNAPSHOT";

    private final LocalEventStore localEventStore;

    public SnapshotLogEntryConsumer(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @Override
    public String entryType() {
        return APPEND_SNAPSHOT;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws Exception {
        TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
        localEventStore.syncSnapshots(groupId, asSerializedTransactionWithToken(transactionWithToken));
    }
}
