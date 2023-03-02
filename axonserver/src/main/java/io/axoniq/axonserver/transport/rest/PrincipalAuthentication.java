/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest;

import io.axoniq.axonserver.access.ApplicationBinding;
import io.axoniq.axonserver.api.Authentication;
import org.jetbrains.annotations.NotNull;
import org.springframework.security.core.userdetails.User;

import java.security.Principal;
import javax.annotation.Nonnull;

import static java.lang.String.format;

/**
 * Implementation of {@link Authentication} that retrieves info from the {@link Principal}
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
public class PrincipalAuthentication implements Authentication {

    private final Principal principal;

    public PrincipalAuthentication(Principal principal) {
        this.principal = principal;
    }

    @Nonnull
    @Override
    public String username() {
        return principal == null ? "<anonymous>" : principal.getName();
    }

    @Override
    public boolean hasRole(@NotNull String role, @NotNull String context) {
        if (principal instanceof org.springframework.security.core.Authentication) {
            String roleAtContext = format("%s@%s", role, context);
            String roleAtAny = format("%s@*", role);
            return ((org.springframework.security.core.Authentication) principal)
                    .getAuthorities()
                    .stream()
                    .anyMatch(grantedAuthority ->
                                      grantedAuthority.getAuthority().equals(roleAtContext)
                                              || grantedAuthority.getAuthority().equals(roleAtAny));
        }
        return false;
    }

    @Override
    public boolean application() {
        return (principal instanceof org.springframework.security.core.Authentication
                && ((org.springframework.security.core.Authentication) principal).getPrincipal() instanceof ApplicationBinding);
    }

    @Override
    public String toString() {
        return "PrincipalAuthentication{" +
                "principal=" + principal +
                '}';
    }

    @Override
    public boolean isLocallyManaged() {
        return application() || isLocalUser();
    }

    private boolean isLocalUser() {
        return (principal instanceof org.springframework.security.core.Authentication
                && ((org.springframework.security.core.Authentication) principal).getPrincipal() instanceof User);
    }

    @Override
    public boolean hasAnyRole(@NotNull String context) {
        if (principal instanceof org.springframework.security.core.Authentication) {
            String atContext = format("@%s", context);
            return ((org.springframework.security.core.Authentication) principal)
                    .getAuthorities()
                    .stream()
                    .anyMatch(grantedAuthority ->
                                      grantedAuthority.getAuthority().endsWith(atContext)
                                              || grantedAuthority.getAuthority().endsWith("@*"));
        }
        return false;
    }
}
