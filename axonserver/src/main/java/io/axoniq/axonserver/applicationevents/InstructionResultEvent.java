/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.admin.Result;

/**
 * Event published when an instruction result is received from an instruction handler.
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class InstructionResultEvent implements AxonServerEvent {

    private final String instructionId;
    private final String clientId;
    private final Result result;
    private final String errorCode;
    private final String errorMessage;

    /**
     * Creates an instance based on the given parameters.
     *
     * @param instructionId the unique id of the instruction
     * @param clientId      the id of the client that handled the instruction
     * @param result        {@code SUCCESS} if the instruction was handled successfully, {@code ACK} if the instruction
     *                      only is acknowledged and no final result is expected, {@code FAILURE} otherwise
     * @param errorCode     the error code in case of a failure during the handling of the instruction
     * @param errorMessage  the error message in case of a failure during the handling of the instruction
     */
    public InstructionResultEvent(String instructionId,
                                  String clientId,
                                  Result result,
                                  String errorCode,
                                  String errorMessage) {
        this.instructionId = instructionId;
        this.clientId = clientId;
        this.result = result;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * Returns the unique id of the instruction.
     *
     * @return the unique id of the instruction
     */
    public String instructionId() {
        return instructionId;
    }

    /**
     * Returns the client id of the client that handled the instruction.
     *
     * @return the client id of the client that handled the instruction
     */
    public String clientId() {
        return clientId;
    }

    /**
     * Returns {@code true} if the instruction was handled successfully, {@code false} otherwise.
     *
     * @return {@code true} if the instruction was handled successfully, {@code false} otherwise
     */
    public boolean isSuccess() {
        return Result.SUCCESS.equals(result);
    }

    public Result result() {
        return result;
    }

    /**
     * Returns the error code of the failure in case that the instruction was not handled successfully.
     *
     * @return the error code of the failure in case that the instruction was not handled successfully
     */
    public String errorCode() {
        return errorCode;
    }

    /**
     * Returns the error message of the failure in case that the instruction was not handled successfully.
     *
     * @return the error message of the failure in case that the instruction was not handled successfully
     */
    public String errorMessage() {
        return errorMessage;
    }
}
