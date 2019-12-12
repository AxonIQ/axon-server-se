package io.axoniq.axonserver.grpc;

import io.grpc.BindableService;

/**
 * @author Marc Gathier
 */
public interface AxonServerInternalService extends BindableService {

    default boolean requiresContextInterceptor() {
        return false;
    }
}
