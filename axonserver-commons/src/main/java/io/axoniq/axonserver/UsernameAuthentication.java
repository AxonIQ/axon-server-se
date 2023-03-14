/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.api.Authentication;
import org.jetbrains.annotations.NotNull;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class UsernameAuthentication implements Authentication {

    private final String username;

    public UsernameAuthentication(String username) {
        this.username = username;
    }

    @NotNull
    @Override
    public String username() {
        return username;
    }

    @Override
    public boolean hasRole(@NotNull String role, @NotNull String context) {
        return false;
    }

    @Override
    public boolean application() {
        return false;
    }

    @Override
    public boolean isLocallyManaged() {
        return true;
    }

    @Override
    public boolean hasAnyRole(@NotNull String context) {
        return false;
    }
}
