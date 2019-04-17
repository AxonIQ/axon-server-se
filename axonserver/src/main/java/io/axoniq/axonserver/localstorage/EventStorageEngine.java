/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import org.springframework.boot.actuate.health.Health;
import org.springframework.data.util.CloseableIterator;

import java.util.Iterator;
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
 */
public interface EventStorageEngine {

    /**
     * Initializes the storage engine.
     * @param validate perform validations on the existing data
     */
    void init(boolean validate);

    /**
     * Prepare a list of events for storing in the event store. Determines the first token for the group of events.
     * @param eventList list of events
     * @return prepared transaction, containing all information to store the events
     */
    PreparedTransaction prepareTransaction(List<SerializedEvent> eventList);

    /**
     * Stores the {@link PreparedTransaction}.
     * Completes the returned completable future when the write is confirmed.
     * @param eventList the prepared transaction
     * @return completable future containing the first token from the prepared transaction
     */
    default CompletableFuture<Long> store(PreparedTransaction eventList) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        completableFuture.completeExceptionally(new UnsupportedOperationException("Cannot create writable datafile"));
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
     * Retrieves the last sequence number for a specific aggregate. Returns empty optional when aggregate is not found.
     * @param aggregateIdentifier the aggregate identifier
     * @return the last sequence number
     */
    Optional<Long> getLastSequenceNumber(String aggregateIdentifier);

    /**
     * Close the storage engine. Free all resources used by the storage engine.
     */
    default void close() {
    }

    /**
     * Retrieves the last event for a specific aggregate id with a sequence number higher than or equal to the given sequence number.
     * Returns empty optional if aggregate is not found or no event with higher sequence number is found.
     * @param aggregateId the aggregate identifier
     * @param minSequenceNumber the minimum sequence number
     * @return optional containing the latest event
     */
    Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber);

    /**
     * Reserve the sequence numbers of the aggregates in the provided list to avoid collisions during store.
     * @param events list of events to store
     */
    default void reserveSequenceNumbers(List<SerializedEvent> events) {
    }

    /**
     * Find events for an aggregate and execute the consumer for each event. Stops when last event for aggregate is found.
     * @param aggregateId the aggregate identifier
     * @param actualMinSequenceNumber the first sequence number to retrieve
     * @param eventConsumer the consumer to apply for each event
     */
    void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber,
                                   Consumer<SerializedEvent> eventConsumer);

    /**
     * Find events for an aggregate and execute the consumer for each event.
     * @param aggregateId the aggregate identifier
     * @param actualMinSequenceNumber the first sequence number to retrieve
     * @param actualMaxSequenceNumber the last sequence number to retrieve
     * @param maxResults maximum number of events to apply
     * @param eventConsumer the consumer to apply for each event
     */
    void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber, long actualMaxSequenceNumber,
                                   int maxResults, Consumer<SerializedEvent> eventConsumer);


    /**
     * Returns the context and the type (event or snapshot) for this storage engine.
     * @return the context and type
     */
    EventTypeContext getType();

    /**
     * Creates an iterator that iterates over the transactions stored in the storage engine.
     * @param firstToken first tracking token to include in the iterator
     * @return iterator of transactions
     */
    Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken);

    /**
     * Creates an iterator that iterates over the transactions stored in the storage engine.
     * @param firstToken first tracking token to include in the iterator
     * @param limitToken last tracking token to include in the iterator (exclusive)
     * @return iterator of transactions
     */
    Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken);

    /**
     * Iterates through the events and calls {@link Predicate} for each event. When the predicate returns false processing stops.
     * @param minToken minumum token of events to process
     * @param minTimestamp minimum timestamp of events to process
     * @param consumer applied for each event
     */
    void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer);

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
     * Adds information to the actuator health endpoint for this event store.
     * @param builder actuator health builder
     */
    default void health(Health.Builder builder) {
    }

    /**
     * Rolls back storage engine to token. Implementations may keep more when token is not at a transaction boundary.
     * @param token the last token to keep.
     */
    default void rollback(long token) {
    }

    /**
     * Removes all reserved sequence numbers from storage engine.
     */
    default void clearReservedSequenceNumbers() {

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
     * @return the next token
     */
    long nextToken();
}
