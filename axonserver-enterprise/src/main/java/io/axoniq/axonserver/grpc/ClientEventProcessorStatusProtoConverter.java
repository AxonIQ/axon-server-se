package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorStatus;

/**
 * Converts between {@link ClientEventProcessorInfo} and {@link ClientEventProcessorStatus} proto messages
 * @author Marc Gathier
 */
public class ClientEventProcessorStatusProtoConverter {

    public static ClientEventProcessorStatus toProto(ClientEventProcessorInfo eventProcessorStatus) {
        return ClientEventProcessorStatus.newBuilder()
                                         .setClient(eventProcessorStatus.getClientName())
                                         .setContext(eventProcessorStatus.getContext())
                                         .setEventProcessorInfo(eventProcessorStatus.getEventProcessorInfo())
                                         .build();
    }

    public static ClientEventProcessorInfo fromProto(ClientEventProcessorStatus clientEventProcessorStatus) {
        return new ClientEventProcessorInfo(clientEventProcessorStatus.getClient(), clientEventProcessorStatus.getContext(),
                                        clientEventProcessorStatus.getEventProcessorInfo());
    }
}
