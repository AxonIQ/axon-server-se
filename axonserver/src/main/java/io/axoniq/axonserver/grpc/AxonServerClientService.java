package io.axoniq.axonserver.grpc;

import io.grpc.BindableService;

/**
 * Defines a class as a GRPC bindable service. All components implementing this interface are exposed through the {@link Gateway}.
 * @author Marc Gathier
 */
public interface AxonServerClientService extends BindableService {

}
