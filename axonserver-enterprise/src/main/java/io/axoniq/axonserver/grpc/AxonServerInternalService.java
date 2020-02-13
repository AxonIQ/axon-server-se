package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.KeepNames;
import io.grpc.BindableService;

/**
 * @author Marc Gathier
 */
@KeepNames
public interface AxonServerInternalService extends BindableService {

    default boolean requiresContextInterceptor() {
        return false;
    }
}
