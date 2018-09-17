package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.EventProcessorEvents;
import io.axoniq.axonserver.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.internal.grpc.ClientEventProcessor;
import io.axoniq.axonserver.internal.grpc.ClientEventProcessorSegment;
import io.axoniq.axonserver.internal.grpc.ConnectorCommand;
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
