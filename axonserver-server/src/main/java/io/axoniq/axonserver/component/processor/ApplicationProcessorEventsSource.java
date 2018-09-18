package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.internal.grpc.ClientEventProcessorStatus;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase.EVENT_PROCESSOR_INFO;

/**
 * Created by Sara Pellegrini on 27/03/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ApplicationProcessorEventsSource {

    private final PlatformService platformService;

    private final ApplicationEventPublisher applicationEventPublisher;

    public ApplicationProcessorEventsSource(PlatformService platformService,
                                            ApplicationEventPublisher applicationEventPublisher) {
        this.platformService = platformService;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @PostConstruct
    public void init(){
        platformService.onInboundInstruction(EVENT_PROCESSOR_INFO, this::onEventProcessorUpdated);
    }

    private void onEventProcessorUpdated(String clientName, String context, PlatformInboundInstruction instruction){
        ClientEventProcessorStatus processorStatus = ClientEventProcessorStatus
                .newBuilder()
                .setClient(clientName)
                .setContext(context)
                .setEventProcessorInfo(instruction.getEventProcessorInfo())
                .build();
        applicationEventPublisher.publishEvent(new EventProcessorStatusUpdate(processorStatus, false));
    }

    public void pauseProcessorRequest(String clientName, String processorName){
        applicationEventPublisher.publishEvent(new PauseEventProcessorRequest(clientName, processorName,false));
    }

    public void startProcessorRequest(String clientName, String processorName){
        applicationEventPublisher.publishEvent(new StartEventProcessorRequest(clientName, processorName, false));
    }

    public void releaseSegment(String clientName, String processorName, int segmentId) {
        applicationEventPublisher.publishEvent(new ReleaseSegmentRequest(clientName, processorName, segmentId, false));
    }
}
