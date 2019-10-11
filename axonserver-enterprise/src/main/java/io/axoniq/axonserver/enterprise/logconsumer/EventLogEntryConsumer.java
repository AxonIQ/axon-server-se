package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter.asSerializedTransactionWithToken;

/**
 * Appends events.
 *
 * @author Marc Gathier
 */
@Component
public class EventLogEntryConsumer implements LogEntryConsumer {

    public static final String LOG_ENTRY_TYPE = "Append.EVENT";

    private final Logger logger = LoggerFactory.getLogger(EventLogEntryConsumer.class);
    private final LocalEventStore localEventStore;

    public EventLogEntryConsumer(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @Override
    public String entryType() {
        return LOG_ENTRY_TYPE;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject()
                                                                                    .getData());
        if (logger.isTraceEnabled()) {
            logger.trace("Index {}: Received Event with index: {} and {} events",
                         e.getIndex(),
                         transactionWithToken.getToken(),
                         transactionWithToken.getEventsCount()
            );
        }
        localEventStore.initContext(groupId, false);
        localEventStore.syncEvents(groupId, asSerializedTransactionWithToken(transactionWithToken));
    }
}
