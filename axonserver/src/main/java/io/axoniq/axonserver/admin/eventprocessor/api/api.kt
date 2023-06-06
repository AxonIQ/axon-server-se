/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.api

import io.axoniq.axonserver.api.Authentication
import io.axoniq.axonserver.component.processor.balancing.LoadBalancingStrategy
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.*


/**
 * Component to perform operations related to event processors.
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
interface EventProcessorAdminService {


    /**
     * Returns the Flux of identifiers of all clients containing a certain event processor.
     *
     * @param identifier the {@link EventProcessorId} to identify the event processor
     * @param authentication info about the authenticated user
     */
    fun clientsBy(identifier: EventProcessorId, authentication: Authentication): Flux<String>

    /**
     * Returns the Flux of the {@link EventProcessor}s for the specified component.
     *
     * @param component the component that contains the event processors
     * @param authentication info about the authenticated user
     */
    fun eventProcessorsByComponent(component: String, authentication: Authentication): Flux<EventProcessor>

    /**
     * Returns the Flux of all known {@link EventProcessor}s.
     *
     * @param authentication info about the authenticated user
     */
    fun eventProcessors(authentication: Authentication): Flux<EventProcessor>

    /**
     * Handles a request to pause a certain event processor.
     * The returned {@link Mono} completes when the request has been propagated to the clients.
     * It doesn't guarantee that the request has been processed by all clients.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun pause(identifier: EventProcessorId, authentication: Authentication): Mono<Result>

    /**
     * Handles a request to start a certain event processor.
     * The returned {@link Mono} completes when the request has been propagated to the clients.
     * It doesn't guarantee that the request has been processed by all clients.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun start(identifier: EventProcessorId, authentication: Authentication): Mono<Result>

    /**
     * Handles a request to split the biggest segment of a certain event processor.
     * The returned {@link Mono} completes when the request has been propagated to the interested client.
     * It doesn't guarantee that the request has been processed by the client.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun split(identifier: EventProcessorId, authentication: Authentication): Mono<Result>

    /**
     * Handles a request to merge the two smallest segments of a certain event processor.
     * The returned {@link Mono} completes when the request has been propagated to the interested clients.
     * It doesn't guarantee that the request has been processed by the clients.
     *
     * @param identifier     the event processor identifier
     * @param authentication info about the authenticated user
     */
    fun merge(identifier: EventProcessorId, authentication: Authentication): Mono<Result>

    /**
     * Handles a request to move a segment from the client that claimed it to the target client.
     * The returned {@link Mono} completes when the request has been propagated to the interested clients.
     * It doesn't guarantee that the request has been processed the clients.
     *
     * @param identifier     the event processor identifier
     * @param segment        the segment to move
     * @param target         the client that should claim the segment
     * @param authentication info about the authenticated user
     */
    fun move(identifier: EventProcessorId, segment: Int, target: String, authentication: Authentication): Mono<Result>

    /**
     * Balance the load for the specified event processor among the connected client.
     *
     * @param identifier     the event processor identifier
     * @param strategy         the strategy to be used to balance the load
     */
    fun loadBalance(identifier: EventProcessorId, strategy: String, authentication: Authentication): Mono<Void>

    /**
     * Sets autoload balance for the specified event processor.
     *
     * @param identifier     the event processor identifier
     * @param strategy         the strategy to be used to balance the load
     */
    fun setAutoLoadBalanceStrategy(identifier: EventProcessorId,
                                   strategy: String,
                                   authentication: Authentication): Mono<Void>

    /**
     * Returns available load balancing strategies.
     *
     * @param authentication info about the authenticated user
     * @return the available load balancing strategies
     */
    fun getBalancingStrategies(authentication: Authentication): Iterable<LoadBalancingStrategy?>
}

interface Result {

    fun isSuccess(): Boolean
    fun isAccepted(): Boolean
}

/**
 * Identifier for event processor.
 */
interface EventProcessorId {

    /**
     * Returns event processor name
     */
    fun name(): String

    /**
     * Returns token store identifier
     */
    fun tokenStoreIdentifier(): String

    /**
     * Returns context of the event processor
     */
    fun context(): String;
}

interface EventProcessor {

    /**
     * Returns the event processor identifier
     */
    fun id(): EventProcessorId

    /**
     * Returns true if the event processor is Streaming, false otherwise.
     */
    fun isStreaming(): Boolean

    /**
     * Returns the mode of the event processor
     */
    fun mode(): String

    /**
     * Returns the instances that run the event processor, one for each client registered on AxonServer
     */
    fun instances(): Iterable<EventProcessorInstance>

    /**
     * Returns the current load balancing strategy name for the event processor
     */
    fun loadBalancingStrategyName(): String?
}

interface EventProcessorInstance {

    /**
     * Returns the client identifier
     */
    fun clientId(): String

    /**
     * Returns true if the instance of the event processor is running, false otherwise
     */
    fun isRunning(): Boolean

    /**
     * Returns the max number of segments the instance can claim
     */
    fun maxCapacity(): Int

    /**
     * Returns the segments that the instance claimed
     */
    fun claimedSegments(): Iterable<EventProcessorSegment>
}

interface EventProcessorSegment {

    /**
     * Returns the segment id.
     */
    fun id(): Int

    /**
     * Returns the denominator of the unit fraction that represents the segment size
     */
    fun onePartOf(): Int

    /**
     * Returns the identifier of the client that claimed the segment
     */
    fun claimedBy(): String

    /**
     * Returns true if the segment is caught up, false otherwise
     */
    fun isCaughtUp(): Boolean

    /**
     * Returns true if the segment is replaying, false otherwise
     */
    fun isReplaying(): Boolean

    /**
     * Returns the lowest token that has already been handled
     */
    fun tokenPosition(): Long

    /**
     * Returns true if the segments is in error, false otherwise
     */
    fun isInError(): Boolean

    /**
     * Returns the optional error, if any
     */
    fun error(): Optional<String>
}