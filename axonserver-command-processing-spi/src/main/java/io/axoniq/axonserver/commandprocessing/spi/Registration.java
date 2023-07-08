/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocessing.spi;

import reactor.core.publisher.Mono;

/**
 * A general purpose registration. Provides a mean to cancel a registration from a component.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
@FunctionalInterface
public interface Registration {

    /**
     * Cancels the registration.
     *
     * @return a {@link Mono} which will complete when cancellation is done.
     */
    Mono<Void> cancel();
}
