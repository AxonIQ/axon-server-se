package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;

/**
 * Responsible to publish an {@link PlatformOutboundInstruction} to the specified client.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public interface InstructionPublisher {

    /**
     * Publishes an {@link PlatformOutboundInstruction} to the specified client
     *
     * @param clientId    the id of the client application
     * @param instruction the instruction to be sent to the client
     */
    void publish(String clientId, PlatformOutboundInstruction instruction);
}
