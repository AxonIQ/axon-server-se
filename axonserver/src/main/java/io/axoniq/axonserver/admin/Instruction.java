/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import io.axoniq.axonserver.ActiveRequestsCache;
import io.axoniq.axonserver.exception.ErrorCode;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface Instruction extends ActiveRequestsCache.Completable {

    long timestamp();

    String description();

    void on(InstructionResult result);

    void completeExceptionally(ErrorCode errorCode, String message);

    boolean isWaitingFor(String clientStreamId);
}
