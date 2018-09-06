package io.axoniq.axonhub.cluster;

import io.axoniq.axonhub.EventProcessorEvents;
import io.axoniq.axonhub.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonhub.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonhub.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonhub.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonhub.grpc.Publisher;
import io.axoniq.axonhub.internal.grpc.ClientEventProcessor;
import io.axoniq.axonhub.internal.grpc.ClientEventProcessorSegment;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Created by Sara Pellegrini on 05/04/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class EventProcessorSynchronizer {

    private final Publisher<ConnectorCommand> clusterMessagePublisher;

    public EventProcessorSynchronizer(Publisher<ConnectorCommand> clusterMessagePublisher) {
        this.clusterMessagePublisher = clusterMessagePublisher;
    }

    @EventListener(condition = "!#a0.proxied")
    public void onEventProcessorInfo(EventProcessorStatusUpdate evt) {
        ConnectorCommand connectorCommand = ConnectorCommand.newBuilder()
                                                            .setClientEventProcessorStatus(evt.eventProcessorStatus())
                                                            .build();
        clusterMessagePublisher.publish(connectorCommand);
    }

    @EventListener(condition = "!#a0.proxied")
    public void onPauseEventProcessorRequest(PauseEventProcessorRequest evt) {
        ConnectorCommand command = ConnectorCommand
                .newBuilder()
                .setPauseClientEventProcessor(ClientEventProcessor
                                                      .newBuilder()
                                                      .setClient(evt.clientName())
                                                      .setProcessorName(evt.processorName())
                )
                .build();

        clusterMessagePublisher.publish(command);
    }

    @EventListener(condition = "!#a0.proxied")
    public void onStartEventProcessorRequest(StartEventProcessorRequest evt) {
        ConnectorCommand command = ConnectorCommand
                .newBuilder()
                .setStartClientEventProcessor(ClientEventProcessor
                                                      .newBuilder()
                                                      .setClient(evt.clientName())
                                                      .setProcessorName(evt.processorName())
                )
                .build();
        clusterMessagePublisher.publish(command);
    }

    @EventListener(condition = "!#a0.proxied")
    public void onReleaseEventProcessor(ReleaseSegmentRequest evt) {
        ConnectorCommand command = ConnectorCommand
                .newBuilder()
                .setReleaseSegment(ClientEventProcessorSegment
                                           .newBuilder()
                                           .setClient(evt.clientName())
                                           .setProcessorName(evt.processorName())
                                           .setSegmentIdentifier(evt.segmentId())
                )
                .build();
        clusterMessagePublisher.publish(command);
    }

    @EventListener(condition = "!#a0.proxied")
    public void onProcessorStatusRequest(EventProcessorEvents.ProcessorStatusRequest evt) {
        ConnectorCommand command = ConnectorCommand
                .newBuilder()
                .setRequestProcessorStatus(ClientEventProcessor
                                           .newBuilder()
                                           .setClient(evt.clientName())
                                           .setProcessorName(evt.processorName())
                )
                .build();
        clusterMessagePublisher.publish(command);
    }
}
