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
import org.springframework.security.core.GrantedAuthority;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class TokenAuthentication implements Authentication {

    private final boolean authenticated;
    private final String name;
    private final Set<GrantedAuthority> roles;
    private final Object details;

    public TokenAuthentication(boolean authenticated, String name, Set<String> roles, Object details) {
        this.authenticated = authenticated;
        this.name = name;
        this.details = details;
        this.roles = roles.stream()
                          .map(s -> (GrantedAuthority) () -> s)
                          .collect(Collectors.toSet());
    }

    public TokenAuthentication(boolean authenticated, String name, Set<String> roles) {
        this(authenticated, name, roles, null);
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return roles;
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getDetails() {
        return details;
    }

    @Override
    public Object getPrincipal() {
        return name;
    }

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public void setAuthenticated(boolean b) {
        // authenticated is only set in constructor
    }

    @Override
    public String getName() {
        return name;
    }
}
