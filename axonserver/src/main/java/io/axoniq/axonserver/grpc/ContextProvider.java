package io.axoniq.axonserver.grpc;

/**
 * Provider to return the current context. Default implementation {@link GrpcContextProvider} retrieves the information from GRPC context, other implementations
 * are made in testcases.
 *
 * @author Marc Gathier
 */
public interface ContextProvider {
    String getContext();
}
