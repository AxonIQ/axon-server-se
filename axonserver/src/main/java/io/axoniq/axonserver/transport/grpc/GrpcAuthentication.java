/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
        String roleAtContext = format("%s@%s", role, context);
        String roleAtAny = format("%s@%s", role, context);
        return authentication.getAuthorities()
                             .stream()
                             .anyMatch(grantedAuthority ->
                                               grantedAuthority.getAuthority().equals(roleAtContext) ||
                                                       grantedAuthority.getAuthority().equals(roleAtAny));
    }

    @Override
    public boolean application() {
        return true;
    }

    @Override
    public boolean isLocallyManaged() {
        return true;
    }

    @Override
    public boolean hasAnyRole(@NotNull String context) {
        String atContext = format("@%s", context);
        return authentication.getAuthorities()
                             .stream()
                             .anyMatch(grantedAuthority ->
                                               grantedAuthority.getAuthority().endsWith(atContext) ||
                                                       grantedAuthority.getAuthority().endsWith("@*"));
    }
}


