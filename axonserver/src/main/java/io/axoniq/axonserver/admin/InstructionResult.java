/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin;

/**
 * The result of the execution of an instraction from an handler.
 *
 * @author Sara Pellegrini
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface InstructionResult {

    /**
     * Returns the identifier of the client that executed the instruction
     *
     * @return the identifier of the client that executed the instruction
     */
    String clientId();

    /**
     * Returns the {@link Result} of the execution of the instruction.
     *
     * @return the {@link Result} of the execution of the instruction
     */
    Result result();

    /**
     * Returns the error code in case of error
     *
     * @return the error code in case of error
     */
    String errorCode();

    /**
     * Returns the error message in case of error
     *
     * @return the error message in case of error
     */
    String errorMessage();
}
