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
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorStatus;
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
     * events and propagates these towards the rest of the cluster, via the given {@code clusterMessagePublisher}.
     *
     * @param clusterMessagePublisher a {@link Publisher} of {@link ConnectorCommand} objects, to publish the handled
     *                                application events towards the rest of the cluster
     */
    public EventProcessorSynchronizer(Publisher<ConnectorCommand> clusterMessagePublisher) {
        this.clusterMessagePublisher = clusterMessagePublisher;
    }

    /**
     * Handle a {@link EventProcessorStatusUpdate} application event, to publish this as a {@link ConnectorCommand}
     * where the {@link ConnectorCommand#getClientEventProcessorStatus()} field is set with a
     * {@link ClientEventProcessorStatus} representing the entire status of that Event Processor.
     *
     * @param event a {@link EventProcessorStatusUpdate} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    @EventListener
    public void on(EventProcessorStatusUpdate event) {
        if( event.isProxied()) return;
        ConnectorCommand connectorCommand =
                ConnectorCommand.newBuilder()
                                .setClientEventProcessorStatus(toProto(event.eventProcessorStatus()))
                                .build();
        clusterMessagePublisher.publish(connectorCommand);
    }

    /**
     * Handle a {@link PauseEventProcessorRequest} application event, to publish this as a {@link ConnectorCommand}
     * where the {@link ConnectorCommand#getPauseClientEventProcessor()} field is set with a
     * {@link ClientEventProcessor} representing the request to pause the given Event Processor.
     *
     * @param event a {@link PauseEventProcessorRequest} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    public void on(PauseEventProcessorRequest event) {
        if( event.isProxied()) return;
        ClientEventProcessor pauseProcessorRequest = ClientEventProcessor.newBuilder()
                                                                         .setClient(event.clientName())
                                                                         .setProcessorName(event.processorName())
                                                                         .build();
        ConnectorCommand connectorCommand = ConnectorCommand.newBuilder()
                                                            .setPauseClientEventProcessor(pauseProcessorRequest)
                                                            .build();
        clusterMessagePublisher.publish(connectorCommand);
    }

    /**
     * Handle a {@link StartEventProcessorRequest} application event, to publish this as a {@link ConnectorCommand}
     * where the {@link ConnectorCommand#getStartClientEventProcessor()} field is set with a
     * {@link ClientEventProcessor} representing the request to start the given Event Processor.
     *
     * @param event a {@link StartEventProcessorRequest} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    public void on(StartEventProcessorRequest event) {
        if( event.isProxied()) return;
        ClientEventProcessor startProcessorRequest = ClientEventProcessor.newBuilder()
                                                                         .setClient(event.clientName())
                                                                         .setProcessorName(event.processorName())
                                                                         .build();
        ConnectorCommand connectorCommand = ConnectorCommand.newBuilder()
                                                            .setStartClientEventProcessor(startProcessorRequest)
                                                            .build();
        clusterMessagePublisher.publish(connectorCommand);
    }

    /**
     * Handle a {@link ReleaseSegmentRequest} application event, to publish this as a {@link ConnectorCommand}
     * where the {@link ConnectorCommand#getReleaseSegment()} field is set with a {@link ClientEventProcessorSegment}
     * representing the right client, processor and segment to release.
     *
     * @param event a {@link ReleaseSegmentRequest} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    public void on(ReleaseSegmentRequest event) {
        if( event.isProxied()) return;

        ClientEventProcessorSegment releaseSegmentRequest =
                ClientEventProcessorSegment.newBuilder()
                                           .setClient(event.getClientName())
                                           .setProcessorName(event.getProcessorName())
                                           .setSegmentIdentifier(event.getSegmentId())
                                           .build();
        clusterMessagePublisher.publish(ConnectorCommand.newBuilder().setReleaseSegment(releaseSegmentRequest).build());
    }

    /**
     * Handle a {@link ProcessorStatusRequest} application event, to publish this as a {@link ConnectorCommand}
     * where the {@link ConnectorCommand#getRequestProcessorStatus()} field is set with a {@link ClientEventProcessor}
     * representing the request for the status of the given Event Processor.
     *
     * @param event a {@link ProcessorStatusRequest} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    @EventListener
    public void on(ProcessorStatusRequest event) {
        if( event.isProxied()) return;
        ClientEventProcessor processorStatusRequest = ClientEventProcessor.newBuilder()
                                                                          .setClient(event.clientName())
                                                                          .setProcessorName(event.processorName())
                                                                          .build();
        ConnectorCommand connectorCommand = ConnectorCommand.newBuilder()
                                                            .setRequestProcessorStatus(processorStatusRequest)
                                                            .build();
        clusterMessagePublisher.publish(connectorCommand);
    }

    /**
     * Handle a {@link SplitSegmentRequest} application event, to publish this as a {@link ConnectorCommand} where the
     * {@link ConnectorCommand#getSplitSegment()} field is set with a {@link ClientEventProcessorSegment} representing
     * the right client, processor and segment to split.
     *
     * @param event a {@link SplitSegmentRequest} to be wrapped in a {@link ConnectorCommand} to be propagated
     *              throughout the rest of the cluster
     */
    @EventListener
    public void on(SplitSegmentRequest event) {
        if( event.isProxied()) return;
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
    @EventListener
    public void on(MergeSegmentRequest event) {
        if( event.isProxied()) return;
        ClientEventProcessorSegment mergeSegmentRequest =
                ClientEventProcessorSegment.newBuilder()
                                           .setClient(event.getClientName())
                                           .setProcessorName(event.getProcessorName())
                                           .setSegmentIdentifier(event.getSegmentId())
                                           .build();
        clusterMessagePublisher.publish(ConnectorCommand.newBuilder().setMergeSegment(mergeSegmentRequest).build());
    }
}
