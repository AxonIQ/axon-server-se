/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import io.axoniq.axonserver.grpc.event.Event;

/**
 * Defines interface for validation actions for transformation operations. Validation actions throw an exception if the
 * validation fails.
 * Implementation may implement additional validations, if needed.
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface TransformationValidator {

    /**
     * Validates a request to delete an event as part of the transformation.
     * Validations:
     * <ul>
     *     <li>transformation id is existing transformation</li>
     *     <li>transformation id is valid for the context</li>
     *     <li>transformation is still open</li>
     *     <li>previous token is token of last provided event for this transformation</li>
     *     <li>token is higher than the previous token</li>
     * </ul>
     * @param context the event store context
     * @param transformationId the identification of the transformation
     * @param token the token (global index) of the event to delete
     * @param previousToken the token of the previous event sent in this transformation (or -1 if this is the first event)
     */
    void validateDeleteEvent(String context, String transformationId, long token, long previousToken);

    /**
     * Validates a request to update an event as part of the transformation.
     * Validations:
     * <ul>
     *     <li>transformation id is existing transformation</li>
     *     <li>transformation id is valid for the context</li>
     *     <li>transformation is still open</li>
     *     <li>previous token is token of last provided event for this transformation</li>
     *     <li>token is higher than the previous token</li>
     *     <li>the aggregate identifier/sequence number match the aggregate identifier/sequence number of the existing event</li>
     * </ul>
     * @param context the event store context
     * @param transformationId the identification of the transformation
     * @param token the token (global index) of the event to delete
     * @param previousToken the token of the previous event sent in this transformation (or -1 if this is the first event)
     * @param event the updated event
     */
    void validateReplaceEvent(String context, String transformationId, long token, long previousToken,
                              Event event);

    /**
     * Validates a request to apply the transformation.
     * Validations:
     * <ul>
     *     <li>transformation id is existing transformation</li>
     *     <li>transformation id is valid for the context</li>
     *     <li>transformation is still open</li>
     *     <li>previous token is token of last provided event for this transformation</li>
     * </ul>
     * @param context the event store context
     * @param transformationId the identification of the transformation
     * @param lastEventToken the token of the last event provided in the transformation
     */
    void apply(String context, String transformationId, long lastEventToken);

    /**
     * Validates a request to cancel a transformation.
     * Validations:
     * <ul>
     *     <li>transformation id is existing transformation</li>
     *     <li>transformation id is valid for the context</li>
     *     <li>transformation is still open</li>
     * </ul>
     * @param context the event store context
     * @param transformationId the identification of the transformation
     */
    void cancel(String context, String transformationId);

    /**
     * Validates a request to delete old versions of event store segments after a transformation.
     * Validations:
     * <ul>
     *     <li>transformation id is existing transformation</li>
     *     <li>transformation id is valid for the context</li>
     *     <li>transformation is done</li>
     *     <li>transformation is applied with keep old versions option</li>
     * </ul>
     * @param context the event store context
     * @param transformationId the identification of the transformation
     */
    void deleteOldVersions(String context, String transformationId);

    /**
     * Validates a request to rollback a transformation.
     * Validations:
     * <ul>
     *     <li>transformation id is existing transformation</li>
     *     <li>transformation id is valid for the context</li>
     *     <li>transformation is done</li>
     *     <li>transformation is applied with keep old versions option</li>
     * </ul>
     * @param context the event store context
     * @param transformationId the identification of the transformation
     */
    void rollback(String context, String transformationId);
}
