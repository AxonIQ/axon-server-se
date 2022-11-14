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
 * Main command request processor interface.
 *
 * @author Milan Savic
 * @author Stefan Dragisic
 * @author Marc Gathier
 */
public interface CommandRequestProcessor {

    Mono<Void> register(CommandHandlerSubscription handler);

    Mono<Void> unregister(String handlerId);

    Mono<CommandResult> dispatch(Command command);

    <T extends Interceptor> Registration registerInterceptor(Class<T> type, T interceptor);
}