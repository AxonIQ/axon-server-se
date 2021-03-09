package io.axoniq.axonserver.grpc;

import java.util.concurrent.Executor;

/**
 * Funcional interface to provide the Executor for flow control operations during gRPC calls.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 * @author Stefan Dragisic
 * @since 4.5
 */
public interface GrpcFlowControlExecutorProvider {

    /**
     * Returns the Executor for the flow control operations during a gRPC call.
     *
     * @return the Executor for the flow control operations during a gRPC call.
     */
    Executor provide();
}
