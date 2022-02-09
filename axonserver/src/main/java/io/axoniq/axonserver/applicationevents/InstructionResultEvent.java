/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

/**
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class InstructionResultEvent implements AxonServerEvent {

    private final String instructionId;
    private final String clientId;
    private final boolean success;
    private final String errorCode;
    private final String errorMessage;

    public InstructionResultEvent(String instructionId,
                                  String clientId,
                                  boolean success,
                                  String errorCode,
                                  String errorMessage) {
        this.instructionId = instructionId;
        this.clientId = clientId;
        this.success = success;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String instructionId() {
        return instructionId;
    }

    public String clientId() {
        return clientId;
    }

    public boolean isSuccess() {
        return success;
    }

    public String errorCode() {
        return errorCode;
    }

    public String errorMessage() {
        return errorMessage;
    }
}
