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
 * Provides the authentication information for a gRPC request. Note that this information is retrieved from
 * the gRPC context, which is only available in the thread where the request was received.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class GrpcContextAuthenticationProvider implements AuthenticationProvider {

    public static final Authentication DEFAULT_PRINCIPAL = new TokenAuthentication(false,
                                                                                   "<anonymous>",
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
