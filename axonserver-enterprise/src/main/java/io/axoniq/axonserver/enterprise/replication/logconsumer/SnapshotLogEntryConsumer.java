package io.axoniq.axonserver.enterprise.replication.logconsumer;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter.asSerializedTransactionWithToken;

/**
 * Appends snapshots to a context in a replication group.
 *
 * @author Milan Savic
 * @since 4.1
 */
@Component
public class SnapshotLogEntryConsumer implements LogEntryConsumer {

    /**
     * The type of log entries the consumer applies.
     */
    public static final String LOG_ENTRY_TYPE = "Append.SNAPSHOT";

    private final LocalEventStore localEventStore;

    public SnapshotLogEntryConsumer(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @Override
    public String entryType() {
        return LOG_ENTRY_TYPE;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws Exception {
        TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
        localEventStore.syncSnapshots(transactionWithToken.getContext().isEmpty() ? groupId : transactionWithToken
                                              .getContext(),
                                      asSerializedTransactionWithToken(transactionWithToken));
    }
}
