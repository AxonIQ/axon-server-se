/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

import io.axoniq.axonserver.ActiveRequestsCache.CancelStrategy;
import io.axoniq.axonserver.admin.InstructionCache.Instruction;
import io.axoniq.axonserver.exception.ErrorCode;

import static java.lang.String.format;

/**
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class CancelOnApplicationDisconnected implements CancelStrategy<Instruction> {

    private final String clientStreamId; //clientId?

    public CancelOnApplicationDisconnected(String clientStreamId) {
        this.clientStreamId = clientStreamId;
    }

    @Override
    public void cancel(String requestId, Instruction request) {
        String message = format("Client %s disconnected during execution of instruction %s", clientStreamId,
                                request.description());
        request.completeExceptionally(ErrorCode.CLIENT_DISCONNECTED, message);
    }

    @Override
    public boolean requestToBeCanceled(String requestId, Instruction request) {
        return request.isWaitingFor(clientStreamId);
    }
}
