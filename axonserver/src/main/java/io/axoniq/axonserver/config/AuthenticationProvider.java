/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import org.springframework.security.core.Authentication;

import java.util.function.Supplier;

/**
 * Component that returns the current authentication context for requests. This provider is only
 * guaranteed if it is used in the same thread as where the request is received.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface AuthenticationProvider extends Supplier<Authentication> {

}
