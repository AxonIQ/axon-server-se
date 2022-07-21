/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import io.axoniq.axonserver.ActiveRequestsCache;
import io.axoniq.axonserver.exception.ErrorCode;

/**
 * Instruction request to be handled.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface Instruction extends ActiveRequestsCache.Completable {

    /**
     * Returns the timestamp of the instruction request.
     *
     * @return the timestamp of the instruction request.
     */
    long timestamp();

    /**
     * Returns the description of the instruction request.
     *
     * @return the description of the instruction request.
     */
    String description();

    /**
     * Handles the received result for the instruction
     *
     * @param result the received result for the instruction
     */
    void on(InstructionResult result);

    /**
     * Completes the instruction request exceptionally.
     *
     * @param errorCode the error code
     * @param message   the error message
     */
    void completeExceptionally(ErrorCode errorCode, String message);

    /**
     * Returns {@code true} if the instruction is waiting a response from the specified client id.
     *
     * @param clientId the identifier of the client that should provide a result
     * @return {@code true} if the instruction is waiting a response from the specified client id,
     * {@code false} otherwise.
     */
    boolean isWaitingFor(String clientId);
}
