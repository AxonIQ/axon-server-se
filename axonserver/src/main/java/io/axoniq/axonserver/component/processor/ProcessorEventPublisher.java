package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.EventTrackerInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.PostConstruct;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase.EVENT_PROCESSOR_INFO;

/**
 * A service to be able to publish specific application events for Event Processor instances, like
 * {@link #startProcessorRequest(String, String)}, {@link #pauseProcessorRequest(String, String)} and
 * {@link #releaseSegment(String, String, int)}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class ProcessorEventPublisher {

    private static final boolean NOT_PROXIED = false;
    private static final boolean PARALLELIZE_STREAM = false;

    private final PlatformService platformService;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final ClientProcessors clientProcessors;

    /**
     * Instantiate a service which published Event Processor specific application events throughout this Axon Server
     * instance.
     *
     * @param platformService           the {@link PlatformService} used to register an inbound instruction on to update
     *                                  Event Processor information
     * @param applicationEventPublisher the {@link ApplicationEventPublisher} used to publish the Event Processor
     *                                  specific application events
     * @param clientProcessors          an {@link Iterable} of
     *                                  {@link io.axoniq.axonserver.component.processor.listener.ClientProcessor}
     *                                  instances used to deduce the segment to be split/merged on the {@link
     *                                  #splitSegment(String, String)} and {@link #mergeSegment(String, String)}
     *                                  operations.
     */
    public ProcessorEventPublisher(PlatformService platformService,
                                   ApplicationEventPublisher applicationEventPublisher,
                                   ClientProcessors clientProcessors) {
        this.platformService = platformService;
        this.applicationEventPublisher = applicationEventPublisher;
        this.clientProcessors = clientProcessors;
    }

    @PostConstruct
    public void init() {
        platformService.onInboundInstruction(EVENT_PROCESSOR_INFO, this::publishEventProcessorStatus);
    }

    private void publishEventProcessorStatus(String clientName,
                                             String context,
                                             PlatformInboundInstruction inboundInstruction) {
        ClientEventProcessorInfo processorStatus =
                new ClientEventProcessorInfo(clientName, context, inboundInstruction.getEventProcessorInfo());
        applicationEventPublisher.publishEvent(new EventProcessorStatusUpdate(processorStatus, NOT_PROXIED));
    }

    public void pauseProcessorRequest(String clientName, String processorName) {
        applicationEventPublisher.publishEvent(new PauseEventProcessorRequest(clientName, processorName, NOT_PROXIED));
    }

    public void startProcessorRequest(String clientName, String processorName) {
        applicationEventPublisher.publishEvent(new StartEventProcessorRequest(clientName, processorName, NOT_PROXIED));
    }

    public void releaseSegment(String clientName, String processorName, int segmentId) {
        applicationEventPublisher.publishEvent(
                new ReleaseSegmentRequest(clientName, processorName, segmentId, NOT_PROXIED)
        );
    }

    public void splitSegment(String clientName, String processorName) {
        applicationEventPublisher.publishEvent(new SplitSegmentRequest(
                NOT_PROXIED, clientName, processorName, deduceSegmentIdToSplit(clientName, processorName)
        ));
    }

    private int deduceSegmentIdToSplit(String clientName, String processorName) {
        return eventTrackerInfoStream(clientName, processorName)
                .min(Comparator.comparingInt(EventTrackerInfo::getOnePartOf))
                .map(EventTrackerInfo::getSegmentId)
                .get();
    }

    public void mergeSegment(String clientName, String processorName) {
        applicationEventPublisher.publishEvent(new MergeSegmentRequest(
                NOT_PROXIED, clientName, processorName, deduceSegmentIdToMerge(clientName, processorName)
        ));
    }

    private int deduceSegmentIdToMerge(String clientName, String processorName) {
        return eventTrackerInfoStream(clientName, processorName)
                .max(Comparator.comparingInt(EventTrackerInfo::getOnePartOf))
                .map(EventTrackerInfo::getSegmentId)
                .get();
    }

    @NotNull
    private Stream<EventTrackerInfo> eventTrackerInfoStream(String clientName, String processorName) {
        return StreamSupport.stream(clientProcessors.spliterator(), PARALLELIZE_STREAM)
                            .filter(clientProcessor -> clientProcessor.clientId().equals(clientName))
                            .filter(clientProcessor -> clientProcessor.eventProcessorInfo().getProcessorName()
                                                                      .equals(processorName))
                            .map(ClientProcessor::spliterator)
                            .flatMap(
                                    segmentSpliterator -> StreamSupport.stream(segmentSpliterator, PARALLELIZE_STREAM)
                            );
    }
}
