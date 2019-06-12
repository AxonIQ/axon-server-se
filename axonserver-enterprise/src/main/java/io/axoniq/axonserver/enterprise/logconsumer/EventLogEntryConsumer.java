package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.grpc.SerializedTransactionWithTokenConverter.asSerializedTransactionWithToken;

/**
 * @author Marc Gathier
 */
@Component
public class EventLogEntryConsumer implements LogEntryConsumer {

    private final Logger logger = LoggerFactory.getLogger(EventLogEntryConsumer.class);
    private final LocalEventStore localEventStore;

    public EventLogEntryConsumer(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) throws InvalidProtocolBufferException {
        if (entryType(e, "Append.EVENT")) {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject()
                                                                                        .getData());
            if (logger.isTraceEnabled()) {
                logger.trace("Index {}: Received Event with index: {} and {} events",
                             e.getIndex(),
                             transactionWithToken.getToken(),
                             transactionWithToken.getEventsCount()
                );
            }
            localEventStore.syncEvents(groupId, asSerializedTransactionWithToken(transactionWithToken));
        } else if (entryType(e, "Append.SNAPSHOT")) {
            TransactionWithToken transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject()
                                                                                        .getData());
            localEventStore.syncSnapshots(groupId, asSerializedTransactionWithToken(transactionWithToken));
        }
    }
}
