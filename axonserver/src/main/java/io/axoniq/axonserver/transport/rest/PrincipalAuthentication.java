package io.axoniq.axonserver.transport.rest;

import io.axoniq.axonserver.access.ApplicationBinding;
import io.axoniq.axonserver.api.Authentication;
import org.jetbrains.annotations.NotNull;

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
                && ((org.springframework.security.core.Authentication)principal).getPrincipal() instanceof ApplicationBinding);
    }
}
