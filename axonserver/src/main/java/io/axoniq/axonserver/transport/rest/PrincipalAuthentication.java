/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import org.springframework.security.core.GrantedAuthority;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
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
            return ((org.springframework.security.core.Authentication)principal).getAuthorities()
                    .stream()
                    .anyMatch(grantedAuthority -> grantedAuthority.getAuthority()
                                                                  .equals(format("%s@%s", role, context)));
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

    public org.springframework.security.core.Authentication wrapped() {
        if (principal instanceof org.springframework.security.core.Authentication) {
            return (org.springframework.security.core.Authentication) principal;
        }

        return new org.springframework.security.core.Authentication() {
            @Override
            public String getName() {
                return principal.getName();
            }

            @Override
            public Collection<? extends GrantedAuthority> getAuthorities() {
                return Collections.emptyList();
            }

            @Override
            public Object getCredentials() {
                return null;
            }

            @Override
            public Object getDetails() {
                return null;
            }

            @Override
            public Object getPrincipal() {
                return principal;
            }

            @Override
            public boolean isAuthenticated() {
                return true;
            }

            @Override
            public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {

            }
        };
    }
}
