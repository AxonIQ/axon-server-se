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
}
