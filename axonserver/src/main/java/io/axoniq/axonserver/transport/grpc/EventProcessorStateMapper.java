package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.admin.eventprocessor.api.ClaimedSegmentState;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorState;
import io.axoniq.axonserver.grpc.admin.ClaimedSegment;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
public class EventProcessorStateMapper implements Function<EventProcessorState, EventProcessor> {

    @Override
    public EventProcessor apply(EventProcessorState eventProcessorState) {
        if (eventProcessorState instanceof EvenProcessorStateMessage) {
            return ((EvenProcessorStateMessage) eventProcessorState).grpcMessage();
        }

        return EventProcessor.newBuilder()
                             .setEventProcessorIdentifier(eventProcessorIdentifier(eventProcessorState.identifier()))
                             .addAllClaimedSegments(claimedSegmentList(eventProcessorState.segments()))
                             .build();
    }

    private EventProcessorIdentifier eventProcessorIdentifier(EventProcessorId eventProcessorId) {
        return EventProcessorIdentifier.newBuilder()
                                       .setProcessorName(eventProcessorId.name())
                                       .setTokenStoreIdentifier(eventProcessorId.tokenStoreIdentifier())
                                       .build();
    }

    private Iterable<? extends ClaimedSegment> claimedSegmentList(List<ClaimedSegmentState> segmentStates) {
        return segmentStates.stream()
                            .map(this::claimedSegment)
                            .collect(Collectors.toList());
    }

    private ClaimedSegment claimedSegment(ClaimedSegmentState claimedSegmentState) {
        return ClaimedSegment.newBuilder()
                             .setClientId(claimedSegmentState.clientId())
                             .setSegmentId(claimedSegmentState.segmentId())
                             .setPartOf(claimedSegmentState.onePartOf())
                             .build();
    }
}
