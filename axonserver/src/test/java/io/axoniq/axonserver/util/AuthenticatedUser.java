/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import io.axoniq.axonserver.api.Authentication;
import org.jetbrains.annotations.NotNull;

public class AuthenticatedUser implements Authentication {

    private final String user;

    public AuthenticatedUser(String user) {
        this.user = user;
    }

    @NotNull
    @Override
    public String username() {
        return user;
    }

    @Override
    public boolean hasRole(@NotNull String role, @NotNull String context) {
        return true;
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
        return true;
    }
}
