/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.PlatformService.InstructionConsumer;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;
import javax.annotation.PostConstruct;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase.EVENT_PROCESSOR_INFO;

/**
 * A service to be able to publish specific application events for Event Processor instances, like {@link
 * #startProcessorRequest(String, String, String, String)}, {@link #pauseProcessorRequest(String, String, String,
 * String)} and {@link #releaseSegment(String, String, String, int, String)}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class ProcessorEventPublisher {

    private static final boolean NOT_PROXIED = false;

    private final Consumer<InstructionConsumer> registerEventProcessorInfoListener;
    private final ApplicationEventPublisher applicationEventPublisher;

    /**
     * Instantiate a service which published Event Processor specific application events throughout this Axon Server
     * instance.
     *
     * @param platformService           the {@link PlatformService} used to register an inbound instruction on to update
     *                                  Event Processor information
     * @param applicationEventPublisher the {@link ApplicationEventPublisher} used to publish the Event Processor
     *                                  specific application events
     */
    @Autowired
    public ProcessorEventPublisher(PlatformService platformService,
                                   ApplicationEventPublisher applicationEventPublisher) {
        this(listener -> platformService.onInboundInstruction(EVENT_PROCESSOR_INFO, listener),
             applicationEventPublisher);
    }

    public ProcessorEventPublisher(
            Consumer<InstructionConsumer> registerEventProcessorInfoListener,
            ApplicationEventPublisher applicationEventPublisher) {
        this.registerEventProcessorInfoListener = registerEventProcessorInfoListener;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @PostConstruct
    public void init() {
        registerEventProcessorInfoListener.accept(this::publishEventProcessorStatus);
    }

    private void publishEventProcessorStatus(PlatformService.ClientComponent clientComponent,
                                             PlatformInboundInstruction inboundInstruction) {
        ClientEventProcessorInfo processorStatus =
                new ClientEventProcessorInfo(clientComponent.getClientId(),
                                             clientComponent.getClientStreamId(),
                                             clientComponent.getContext(), inboundInstruction.getEventProcessorInfo());
        applicationEventPublisher.publishEvent(new EventProcessorStatusUpdate(processorStatus));
    }

    public void pauseProcessorRequest(String context, String clientId, String processorName, String instructionId) {
        applicationEventPublisher.publishEvent(new PauseEventProcessorRequest(context,
                                                                              clientId, processorName,
                                                                              instructionId,
                                                                              NOT_PROXIED));
    }

    public void startProcessorRequest(String context, String clientId, String processorName, String instructionId) {
        applicationEventPublisher.publishEvent(new StartEventProcessorRequest(context,
                                                                              clientId, processorName,
                                                                              instructionId,
                                                                              NOT_PROXIED));
    }

    public void releaseSegment(String context, String clientId, String processorName, int segmentId,
                               String instructionId) {
        applicationEventPublisher.publishEvent(
                new ReleaseSegmentRequest(context, clientId, processorName, segmentId, instructionId, NOT_PROXIED)
        );
    }

    /**
     * Publishes an event to split a segment.
     *
     * @param context       the principal context of the event processor
     * @param clientId      the clientId owning the segment to split
     * @param segmentId     the segment to split
     * @param processorName a {@link String} specifying the Tracking Event Processor for which the biggest segment
     *                      should be split in two
     */
    public void splitSegment(String context, String clientId, String processorName, int segmentId,
                             String instructionId) {
        applicationEventPublisher.publishEvent(new SplitSegmentRequest(
                NOT_PROXIED,
                context,
                clientId,
                processorName,
                segmentId,
                instructionId
        ));
    }

    /**
     * Publishes an event to merge a segment.
     *
     * @param context       the principal context of the event processor
     * @param clientId      the clientId owning the segment to merge
     * @param segmentId     the segment to merge
     * @param processorName a {@link String} specifying the Tracking Event Processor for which the smallest segment
     *                      should be merged with the segment that it's paired with
     */
    public void mergeSegment(String context, String clientId, String processorName, int segmentId,
                             String instructionId) {

        applicationEventPublisher.publishEvent(new MergeSegmentRequest(
                NOT_PROXIED,
                context,
                clientId,
                processorName,
                segmentId,
                instructionId
        ));
    }
}
