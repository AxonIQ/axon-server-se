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
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface InstructionResult {

    String clientId();

    boolean success();

    String errorCode();

    String errorMessage();
}