package io.axoniq.axonserver.enterprise.messaging.event;

import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.message.event.EventStore;
import reactor.core.publisher.Flux;

import java.util.Optional;

/**
 * Extension of the {@link EventStore} interface that adds additional operations to support multi-tier storage.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface LowerTierEventStore extends EventStore {

    /**
     * Creates a flux to read event transactions from a lower tier event store.
     *
     * @param context the name of the context
     * @param from    the token of the first transaction to retrieve
     * @param to      the token of the last event to retrieve (exclusive)
     * @return flux with event transactions
     */
    Flux<TransactionWithToken> eventTransactions(String context, long from, long to);

    /**
     * Creates a flux to read snapshot transactions from a lower tier event store.
     *
     * @param context the name of the context
     * @param from    the token of the first transaction to retrieve
     * @param to      the token of the last snapshot to retrieve (exclusive)
     * @return flux with snapshot transactions
     */
    Flux<TransactionWithToken> snapshotTransactions(String context, long from, long to);

    /**
     * Get the highest sequence nr for an aggregate in a context. Sends additional information to the lower tier
     * event store to optimize the search process.
     *
     * @param context     the name of the context
     * @param aggregateId the identifier of the aggregate
     * @param maxSegments maximum number of segments to search for the aggregate (implementation may ignore this value)
     * @param maxToken    events with tokens higher than this token have already been checked for this aggregate
     * @return sequence number if found
     */
    Optional<Long> getHighestSequenceNr(String context, String aggregateId, int maxSegments, long maxToken);

    /**
     * Retrieves the last event token in the context.
     *
     * @param context the name of the context
     * @return the last event token in the context
     */
    long getLastToken(String context);

    /**
     * Retrieves the last snapshot token in the context.
     *
     * @param context the name of the context
     * @return the last snapshot token in the context
     */
    long getLastSnapshotToken(String context);
}
