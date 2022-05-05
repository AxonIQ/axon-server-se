package io.axoniq.axonserver.transport.grpc.eventprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;

import javax.annotation.Nonnull;

/**
 * Implementation of {@link EventProcessorId} that wraps the Grpc message and accesses to it only if needed.
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
public class EventProcessorIdMessage implements EventProcessorId {

    private final EventProcessorIdentifier grpcMessage;
    private final String context;

    public EventProcessorIdMessage(EventProcessorIdentifier grpcMessage) {
        this.grpcMessage = grpcMessage;
        this.context = "";
    }

    public EventProcessorIdMessage(EventProcessorIdentifier grpcMessage, String context) {
        this.grpcMessage = grpcMessage;
        this.context = context;
    }

    @Nonnull
    @Override
    public String name() {
        return grpcMessage.getProcessorName();
    }


    @Nonnull
    @Override
    public String tokenStoreIdentifier() {
        return grpcMessage.getTokenStoreIdentifier();
    }

    @Nonnull
    @Override
    public String context() {
        return context;
    }
}
