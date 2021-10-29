package io.axoniq.axonserver.transport.rest;

import io.axoniq.axonserver.api.Authentication;

import java.security.Principal;
import javax.annotation.Nonnull;

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
}
