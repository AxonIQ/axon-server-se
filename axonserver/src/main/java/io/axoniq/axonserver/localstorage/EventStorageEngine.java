/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthIndicatorProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.data.util.CloseableIterator;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Defines the interface for implementing a storage engine for events.
 * A single storage engine manages events or snapshots for one context.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public interface EventStorageEngine {

    enum SearchHint {
        RECENT_ONLY
    }

    /**
     * Initializes the storage engine.
     *
     * @param validate perform validations on the existing data
     */
    void init(boolean validate, long defaultFirstIndex);

    default void init(boolean validate) {
        init(validate, 0L);
    }

    /**
     * Stores a number of events.
     * Completes the returned completable future when the write is confirmed.
     *
     * @param eventList list of events
     * @return completable future containing the token of the first stored event
     */
    default CompletableFuture<Long> store(List<Event> eventList) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new UnsupportedOperationException("Store operation not supported"));
        return completableFuture;
    }

    /**
     * Retrieves the last token confirmed in the event store.
     * @return the last confirmed token
     */
    default long getLastToken() {
        return -1;
    }

    /**
     * Retrieves the last sequence number for a specific aggregate. In some implementations
     * searching for a non-existing aggregate may be an expensive operation, in which case you could
     * provide {@link SearchHint} RECENT_ONLY, to only look for the aggregate in recent events (the exact meaning of
     * recent
     * depends on the EventStorageEngine implementation)
     * Returns empty optional when aggregate is not found.
     *
     * @param aggregateIdentifier the aggregate identifier
     * @param searchHints         flags to optimize search process
     * @return the last sequence number
     */
    Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint... searchHints);

    default Optional<Long> getLastSequenceNumber(String aggregateIdentifier, int maxSegmentsHint, long maxTokenHint) {
        return getLastSequenceNumber(aggregateIdentifier);
    }

    /**
     * Close the storage engine. Free all resources used by the storage engine.
     *
     * @param deleteData if true all event data is deleted
     */
    default void close(boolean deleteData) {
    }

    /**
     * Registers a listener that will be called when the storage engine is closed.
     * @param listener the listener to call
     * @return a registration that can be used to unregister the listener
     */
    Registration registerCloseListener(Runnable listener);

    /**
     * Retrieves the last event for a specific aggregate id with a sequence number higher than or equal to the given sequence number.
     * Returns empty optional if aggregate is not found or no event with higher sequence number is found.
     * @param aggregateIdentifier the aggregate identifier
     * @param minSequenceNumber the minimum sequence number
     * @return optional containing the latest event
     */
    Optional<SerializedEvent> getLastEvent(String aggregateIdentifier, long minSequenceNumber);


    /**
     * Find events for an aggregate and execute the consumer for each event. Stops when last event for aggregate is found.
     * @param aggregateId the aggregate identifier
     * @param minSequenceNumber the first sequence number to retrieve
     * @param maxSequenceNumber the last sequence number to retrieve (exlusive)
     * @param eventConsumer     the consumer to apply for each event
     */
    void processEventsPerAggregate(String aggregateId, long minSequenceNumber, long maxSequenceNumber, long minToken,
                                   Consumer<SerializedEvent> eventConsumer);

    /**
     * Find events for an aggregate and execute the consumer for each event. The event with the highest sequence number
     * is returned first.
     *
     * @param aggregateId       the aggregate identifier
     * @param minSequenceNumber the first sequence number to retrieve
     * @param maxSequenceNumber the last sequence number to retrieve
     * @param maxResults        maximum number of events to apply
     * @param eventConsumer     the consumer to apply for each event
     */
    void processEventsPerAggregateHighestFirst(String aggregateId, long minSequenceNumber, long maxSequenceNumber,
                                               int maxResults, Consumer<SerializedEvent> eventConsumer);


    /**
     * Returns the context and the type (event or snapshot) for this storage engine.
     * @return the context and type
     */
    EventTypeContext getType();

    /**
     * Creates an iterator that iterates over the transactions stored in the storage engine.
     *
     * @param firstToken first tracking token to include in the iterator
     * @param limitToken last tracking token to include in the iterator (exclusive)
     * @return iterator of transactions
     */
    CloseableIterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken);

    /**
     * Iterates through the events and calls {@link Predicate} for each event. When the predicate returns false
     * processing stops.
     *
     * @param queryOptions
     * @param consumer     applied for each event
     */
    void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer);

    /**
     * Gets filenames to back up for this storage engine. Only relevant for file based storage.
     * @param lastSegmentBackedUp last segment backed up before
     * @return stream of filenames
     */
    default Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves first token in storage engine.
     * @return first token or -1 when storage is empty
     */
    long getFirstToken();

    /**
     * Retrieves the token of the first event from a specific point in time.
     * Returns -1 when there are no events found after the specified time.
     * @param instant the timestamp
     * @return the token of the first event
     */
    long getTokenAt(long instant);

    /**
     * Rolls back storage engine to token. Implementations may keep more when token is not at a transaction boundary.
     * @param token the last token to keep.
     */
    default void rollback(long token) {
    }

    /**
     * Return a closeable iterator to iterate over all events starting at token start.
     * @param start first token to return
     * @return closeable iterator of SerializedEventWithToken
     */
    CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start);

    /**
     * Version number for new transactions.
     * @return the version number of new transactions in this storage engine.
     */
    default byte transactionVersion() {
        return 0;
    }

    /**
     * Returns the next token that will be used by the event store. Does not change the token.
     *
     * @return the next token
     */
    long nextToken();

    /**
     * Deletes all event data in the Event Store (Only intended for development environments).
     */
    void deleteAllEventData();


    /**
     * Validates that the transaction that is stored at the given {@code token} is the same as the
     * provided events. Throws an {@link io.axoniq.axonserver.exception.EventStoreValidationException} exception when
     * there is a difference.
     *
     * @param token     the token of the first event in the transaction
     * @param eventList the list of events
     */
    default void validateTransaction(long token, List<SerializedEvent> eventList) {
    }

}
