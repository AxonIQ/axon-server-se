/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.grpc.GrpcMetadataKeys;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultAuthenticationProvider implements AuthenticationProvider {

    public static final String UNAUTHORIZED = "Unauthorized";
    public static final Authentication DEFAULT_PRINCIPAL = new TokenAuthentication(false,
                                                                                   UNAUTHORIZED,
                                                                                   Collections.emptySet());
    public static final Authentication ADMIN_PRINCIPAL = new TokenAuthentication(true,
                                                                                 "Admin",
                                                                                 Collections.singleton("ADMIN"));
    public static final Authentication USER_PRINCIPAL = new TokenAuthentication(true, "User", Collections.emptySet());

    @Override
    public Authentication get() {
        Authentication principal = GrpcMetadataKeys.PRINCIPAL_CONTEXT_KEY.get();
        return principal == null ? DEFAULT_PRINCIPAL : principal;
    }
}
