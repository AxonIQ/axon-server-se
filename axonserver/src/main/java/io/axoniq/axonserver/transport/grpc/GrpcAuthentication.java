package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.api.Authentication;

import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * Implementation of {@link Authentication} that retrieves information from Spring Security Authentication
 *
 * @author Stefan Dragisic
 * @author Sara Pellegrini
 * @since 4.6
 */
public class GrpcAuthentication implements Authentication {

    private final org.springframework.security.core.Authentication authentication;

    public GrpcAuthentication(Supplier<org.springframework.security.core.Authentication> authenticationProvider) {
        this.authentication = authenticationProvider.get();
    }

    @Nonnull
    @Override
    public String name() {
        return authentication.getName();
    }
}
