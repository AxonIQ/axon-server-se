package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ProcessorStatusRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessor;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorSegment;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.grpc.ClientEventProcessorStatusProtoConverter.toProto;

/**
 * Service which handles non-proxied application events targeted towards Event Processor instances, to be propagated
 * towards the rest of the cluster through the given {@code clusterMessagePublisher}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class EventProcessorSynchronizer {

    private final Publisher<ConnectorCommand> clusterMessagePublisher;

    /**
     * Instantiate an {@link EventProcessorSynchronizer} which handles internal Event Processor specific application
     * events and propagates these twoards the rest of the cluster, via the given {@code clusterMessagePublisher}.
     *
     * @param clusterMessagePublisher a {@link Publisher} of {@link ConnectorCommand} objects, to publish the handled
     *                                application events towards the rest of the cluster
     */
    public EventProcessorSynchronizer(Publisher<ConnectorCommand> clusterMessagePublisher) {
        this.clusterMessagePublisher = clusterMessagePublisher;
    }

    @EventListener(condition = "!#a0.proxied")
    public void onEventProcessorInfo(EventProcessorStatusUpdate event) {
        ConnectorCommand connectorCommand =
                ConnectorCommand.newBuilder()
                                .setClientEventProcessorStatus(toProto(event.eventProcessorStatus()))
                                .build();
        clusterMessagePublisher.publish(connectorCommand);
    }

    @EventListener(condition = "!#a0.proxied")
    public void onPauseEventProcessorRequest(PauseEventProcessorRequest event) {
        ClientEventProcessor pauseProcessorRequest = ClientEventProcessor.newBuilder()
                                                                         .setClient(event.clientName())
                                                                         .setProcessorName(event.processorName())
                                                                         .build();
        clusterMessagePublisher.publish(
                ConnectorCommand.newBuilder().setPauseClientEventProcessor(pauseProcessorRequest).build()
        );
    }

    @EventListener(condition = "!#a0.proxied")
    public void onStartEventProcessorRequest(StartEventProcessorRequest event) {
        ClientEventProcessor startProcessorRequest = ClientEventProcessor.newBuilder()
                                                                         .setClient(event.clientName())
                                                                         .setProcessorName(event.processorName())
                                                                         .build();
        clusterMessagePublisher.publish(
                ConnectorCommand.newBuilder().setStartClientEventProcessor(startProcessorRequest).build()
        );
    }

    @EventListener(condition = "!#a0.proxied")
    public void onReleaseEventProcessor(ReleaseSegmentRequest event) {
        ClientEventProcessorSegment releaseSegmentRequest =
                ClientEventProcessorSegment.newBuilder()
                                           .setClient(event.getClientName())
                                           .setProcessorName(event.getProcessorName())
                                           .setSegmentIdentifier(event.getSegmentId())
                                           .build();
        clusterMessagePublisher.publish(ConnectorCommand.newBuilder().setReleaseSegment(releaseSegmentRequest).build());
    }

    @EventListener(condition = "!#a0.proxied")
    public void onProcessorStatusRequest(ProcessorStatusRequest event) {
        ClientEventProcessor processorStatusRequest = ClientEventProcessor.newBuilder()
                                                                          .setClient(event.clientName())
                                                                          .setProcessorName(event.processorName())
                                                                          .build();
        clusterMessagePublisher.publish(
                ConnectorCommand.newBuilder().setRequestProcessorStatus(processorStatusRequest).build()
        );
    }

    /**
     * Handle a {@link SplitSegmentRequest} application event, to publish this as a {@link ConnectorCommand} where the
     * {@link ConnectorCommand#getSplitSegment()} field is set with a {@link ClientEventProcessorSegment} representing
     * the right client, processor and segment to split.
     *
     * @param event a {@link SplitSegmentRequest} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    @EventListener(condition = "!#a0.proxied")
    public void on(SplitSegmentRequest event) {
        ClientEventProcessorSegment splitSegmentRequest =
                ClientEventProcessorSegment.newBuilder()
                                           .setClient(event.getClientName())
                                           .setProcessorName(event.getProcessorName())
                                           .setSegmentIdentifier(event.getSegmentId())
                                           .build();
        clusterMessagePublisher.publish(ConnectorCommand.newBuilder().setSplitSegment(splitSegmentRequest).build());
    }

    /**
     * Handle a {@link MergeSegmentRequest} application event, to publish this as a {@link ConnectorCommand} where the
     * {@link ConnectorCommand#getMergeSegment()} field is set with a {@link ClientEventProcessorSegment} representing
     * the right client, processor and segment to be merged.
     *
     * @param event a {@link MergeSegmentRequest} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    @EventListener(condition = "!#a0.proxied")
    public void on(MergeSegmentRequest event) {
        ClientEventProcessorSegment mergeSegmentRequest =
                ClientEventProcessorSegment.newBuilder()
                                           .setClient(event.getClientName())
                                           .setProcessorName(event.getProcessorName())
                                           .setSegmentIdentifier(event.getSegmentId())
                                           .build();
        clusterMessagePublisher.publish(ConnectorCommand.newBuilder().setMergeSegment(mergeSegmentRequest).build());
    }
}
