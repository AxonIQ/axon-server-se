package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.AxonServerEventPublisher;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentsSucceeded;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentsSucceeded;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.control.EventProcessorSegmentReference;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.istruction.result.InstructionResultSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;

/**
 * Service responsible to communicate instructions about event processor management with client applications.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class EventProcessorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessorService.class);

    private final BiConsumer<String, PlatformOutboundInstruction> instructionPublisher;

    private final InstructionResultSource.Factory instructionResultSource;

    private final AxonServerEventPublisher eventPublisher;

    /**
     * Creates an instance of {@link EventProcessorService} based on the {@link PlatformService} as instruction
     * publisher, the specified {@code instructionResultSource} to subscribe to the result of the sent instructions
     * and the specified {@code eventPublisher} to notify internal events in case of positive result for the instruction
     * executions.
     *
     * @param instructionPublisher    used to send instructions to client applications
     * @param instructionResultSource used to subscribe to execution results
     * @param eventPublisher          used to publish internal events
     */
    @Autowired
    public EventProcessorService(PlatformService instructionPublisher,
                                 InstructionResultSource.Factory instructionResultSource,
                                 AxonServerEventPublisher eventPublisher) {
        this(instructionPublisher::sendToClient,
             instructionResultSource,
             eventPublisher);
    }

    /**
     * Creates an instance of {@link EventProcessorService} using the specified {@code instructionPublisher} to sent
     * instructions to client applications, the specified {@code instructionResultSource} to subscribe to the result
     * of the sent instructions and the specified {@code eventPublisher} to notify internal events in case of positive
     * result for the instruction executions.
     *
     * @param instructionPublisher    used to send instructions to client applications
     * @param instructionResultSource used to subscribe to execution results
     * @param eventPublisher          used to publish internal events
     */
    public EventProcessorService(
            BiConsumer<String, PlatformOutboundInstruction> instructionPublisher,
            InstructionResultSource.Factory instructionResultSource,
            AxonServerEventPublisher eventPublisher) {
        this.instructionPublisher = instructionPublisher;
        this.instructionResultSource = instructionResultSource;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Publishes an instruction to the involved client application in order to request the split of a segment.
     * Wait for the result of the sent instruction. In case of success it publishes a {@link SplitSegmentsSucceeded}
     * internal event.
     *
     * @param event the event specifying the split segment request
     */
    @EventListener
    public void on(EventProcessorEvents.SplitSegmentRequest event) {
        EventProcessorSegmentReference splitSegmentRequest =
                EventProcessorSegmentReference.newBuilder()
                                              .setProcessorName(event.getProcessorName())
                                              .setSegmentIdentifier(event.getSegmentId())
                                              .build();

        PlatformOutboundInstruction instruction =
                PlatformOutboundInstruction.newBuilder()
                                           .setSplitEventProcessorSegment(splitSegmentRequest)
                                           .build();
        EventProcessorIdentifier processorId = new NameBasedEventProcessorIdentifier(event.getProcessorName());
        SplitSegmentsSucceeded success = new SplitSegmentsSucceeded(processorId);
        instructionResultSource
                .onInstructionResultFor(instruction.getInstructionId())
                .subscribe(() -> eventPublisher.publishEvent(success),
                           error -> LOGGER.warn("Error during segment split: {}, {}", error, instruction),
                           timeout -> LOGGER.warn("The following operation is taking to long: {}", instruction));

        instructionPublisher.accept(event.getClientName(), instruction);
    }


    /**
     * Publishes an instruction to the involved client application in order to request the merge of a segment.
     * Wait for the result of the sent instruction. In case of success it publishes a {@link MergeSegmentsSucceeded}
     * internal event.
     *
     * @param event the event specifying the merge segment request
     */
    @EventListener
    public void on(EventProcessorEvents.MergeSegmentRequest event) {
        EventProcessorSegmentReference mergeSegmentRequest =
                EventProcessorSegmentReference.newBuilder()
                                              .setProcessorName(event.getProcessorName())
                                              .setSegmentIdentifier(event.getSegmentId())
                                              .build();
        PlatformOutboundInstruction instruction =
                PlatformOutboundInstruction.newBuilder()
                                           .setMergeEventProcessorSegment(mergeSegmentRequest)
                                           .build();
        EventProcessorIdentifier processorId = new NameBasedEventProcessorIdentifier(event.getProcessorName());
        MergeSegmentsSucceeded success = new MergeSegmentsSucceeded(processorId);

        instructionResultSource
                .onInstructionResultFor(instruction.getInstructionId())
                .subscribe(() -> eventPublisher.publishEvent(success),
                           error -> LOGGER.warn("Error during segment merge: {}, {}", error, instruction),
                           timeout -> LOGGER.warn("The following operation is taking to long: {}", instruction));

        instructionPublisher.accept(event.getClientName(), instruction);
    }

    /**
     * Publishes an instruction to the involved client application in order to request the release of a segment.
     *
     * @param event the event specifying the release segment request
     */
    @EventListener
    public void on(EventProcessorEvents.ReleaseSegmentRequest event) {
        EventProcessorSegmentReference releaseSegmentRequest =
                EventProcessorSegmentReference.newBuilder()
                                              .setProcessorName(event.getProcessorName())
                                              .setSegmentIdentifier(event.getSegmentId())
                                              .build();

        PlatformOutboundInstruction outboundInstruction =
                PlatformOutboundInstruction.newBuilder()
                                           .setReleaseSegment(releaseSegmentRequest)
                                           .build();
        instructionPublisher.accept(event.getClientName(), outboundInstruction);
    }
}
