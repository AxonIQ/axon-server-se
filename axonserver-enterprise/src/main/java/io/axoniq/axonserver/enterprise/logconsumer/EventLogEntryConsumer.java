package io.axoniq.axonserver.enterprise.logconsumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.TransactionInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class EventLogEntryConsumer implements LogEntryConsumer {
    private final Logger logger = LoggerFactory.getLogger(EventLogEntryConsumer.class);
    private final LocalEventStore localEventStore;

    public EventLogEntryConsumer(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @Override
    public void consumeLogEntry(String groupId, Entry e) {
        if (e.hasSerializedObject()) {
            logger.debug("{}: received type: {}", groupId, e.getSerializedObject().getType());
            if (e.getSerializedObject().getType().equals("Append.EVENT")) {
                TransactionWithToken transactionWithToken = null;
                try {
                    transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
                    if( logger.isTraceEnabled()) {
                        logger.trace("Index {}: Received Event with index: {} and {} events",
                                    e.getIndex(),
                                    transactionWithToken.getIndex(),
                                    transactionWithToken.getEventsCount()
                        );
                    }
                    if (transactionWithToken.getIndex() > localEventStore.getLastEventIndex(groupId)) {
                        localEventStore.syncEvents(groupId, new TransactionInformation(transactionWithToken.getIndex()), transactionWithToken);
                    } else {
                        logger.debug("Index {}: event already applied",
                                    e.getIndex());
                    }
                } catch (InvalidProtocolBufferException e1) {
                    throw new RuntimeException("Error processing entry: " + e.getIndex(), e1);
                }
            } else if (e.getSerializedObject().getType().equals("Append.SNAPSHOT")) {
                TransactionWithToken transactionWithToken = null;
                try {
                    transactionWithToken = TransactionWithToken.parseFrom(e.getSerializedObject().getData());
                    if (transactionWithToken.getIndex() > localEventStore.getLastSnapshotIndex(groupId)) {
                        localEventStore.syncSnapshots(groupId, new TransactionInformation(transactionWithToken.getIndex()), transactionWithToken);
                    } else {
                        logger.debug("Index {}: snapshot already applied",
                                    e.getIndex());
                    }
                } catch (InvalidProtocolBufferException e1) {
                    throw new RuntimeException("Error processing entry: " + e.getIndex(), e1);
                }

            }
        }

    }

    @Override
    public int priority() {
        return 0;
    }
}
