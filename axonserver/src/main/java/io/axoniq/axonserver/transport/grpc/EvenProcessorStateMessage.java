package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.admin.eventprocessor.api.ClaimedSegmentState;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorState;
import io.axoniq.axonserver.grpc.admin.ClaimedSegment;
import io.axoniq.axonserver.grpc.admin.EventProcessor;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
public class EvenProcessorStateMessage implements EventProcessorState {

    private final EventProcessor grpcMessage;

    public EvenProcessorStateMessage(EventProcessor grpcMessage) {
        this.grpcMessage = grpcMessage;
    }

    @Nonnull
    @Override
    public EventProcessorId identifier() {
        return new EventProcessorIdMessage(grpcMessage.getEventProcessorIdentifier());
    }

    @Nonnull
    @Override
    public List<ClaimedSegmentState> segments() {
        List<ClaimedSegment> claimedSegmentsList = grpcMessage.getClaimedSegmentsList();
        return claimedSegmentsList.stream()
                                  .map(ClaimedSegmentStateMessage::new)
                                  .collect(Collectors.toList());
    }

    public EventProcessor grpcMessage() {
        return grpcMessage;
    }
}

class ClaimedSegmentStateMessage implements ClaimedSegmentState {

    private final ClaimedSegment grpcMessage;

    ClaimedSegmentStateMessage(ClaimedSegment grpcMessage) {
        this.grpcMessage = grpcMessage;
    }

    @Nonnull
    @Override
    public String clientId() {
        return grpcMessage.getClientId();
    }

    @Override
    public int segmentId() {
        return grpcMessage.getSegmentId();
    }

    @Override
    public int onePartOf() {
        return grpcMessage.getPartOf();
    }
}