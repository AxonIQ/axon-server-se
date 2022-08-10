package io.axoniq.axonserver.transport.rest;

import io.axoniq.axonserver.api.Authentication;
import org.jetbrains.annotations.NotNull;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;

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
        if (principal instanceof UsernamePasswordAuthenticationToken) {
            return ((UsernamePasswordAuthenticationToken)principal).getAuthorities()
                                                            .stream()
                                                            .anyMatch(grantedAuthority -> grantedAuthority.toString()
                                                                                                          .equals(format("%s@%s", role, context)));
        }
        return false;
    }

    @Override
    public boolean application() {
        return ! (principal instanceof UsernamePasswordAuthenticationToken);
    }
}
