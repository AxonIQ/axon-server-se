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
import io.axoniq.axonserver.exception.ErrorCode;

import static java.lang.String.format;

/**
 * {@link CancelStrategy} implementation to cancel a request when the handling client disconnects.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class CancelOnHandlerDisconnected implements CancelStrategy<Instruction> {

    private final String clientId;

    /**
     * Constructs an instance base on the specified client id.
     *
     * @param clientId the identifier of the handling client.
     */
    CancelOnHandlerDisconnected(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public void cancel(String requestId, Instruction request) {
        String message = format("Client %s disconnected during execution of instruction %s", clientId,
                                request.description());
        request.completeExceptionally(ErrorCode.CLIENT_DISCONNECTED, message);
    }

    @Override
    public boolean requestToBeCanceled(String requestId, Instruction request) {
        return request.isWaitingFor(clientId);
    }
}
