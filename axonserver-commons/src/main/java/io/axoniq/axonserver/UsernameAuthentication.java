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
}
