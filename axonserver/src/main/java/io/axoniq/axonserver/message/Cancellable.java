/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

/**
 * Allow to cancel a specific operation.
 *
 * @author Sara Pellegrini
 * @since 4.6.10
 */
public interface Cancellable {

    /**
     * Cancels the operation.
     *
     * @return {@code true} if it was possible to cancel the operation, {@code false} otherwise
     */
    boolean cancel();
}
