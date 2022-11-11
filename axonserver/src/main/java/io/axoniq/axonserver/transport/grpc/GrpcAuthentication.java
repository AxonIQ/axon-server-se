/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.api.Authentication;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;
import javax.annotation.Nonnull;

import static java.lang.String.format;

/**
 * Implementation of {@link Authentication} that retrieves information from Spring Security Authentication
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
public class GrpcAuthentication implements Authentication {

    private final org.springframework.security.core.Authentication authentication;

    public GrpcAuthentication(Supplier<org.springframework.security.core.Authentication> authenticationProvider) {
        this.authentication = authenticationProvider.get();
    }

    @Nonnull
    @Override
    public String username() {
        return authentication.getName();
    }

    @Override
    public boolean hasRole(@NotNull String role, @NotNull String context) {
        return authentication.getAuthorities()
                             .stream()
                             .anyMatch(grantedAuthority -> grantedAuthority.getAuthority()
                                                                           .equals(format("%s@%s", role, context)));
    }

    @Override
    public boolean application() {
        return true;
    }

    public org.springframework.security.core.Authentication wrapped() {
        return authentication;
    }
}


