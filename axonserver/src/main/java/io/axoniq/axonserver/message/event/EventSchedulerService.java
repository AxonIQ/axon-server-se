/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.CancelScheduledEventRequest;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TaskPayload;
import io.axoniq.axonserver.taskscheduler.TaskPayloadSerializer;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of the {@link EventSchedulerService}.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class EventSchedulerService extends EventSchedulerGrpc.EventSchedulerImplBase implements
        AxonServerClientService {

    private final StandaloneTaskManager standaloneTaskManager;
    private final TaskPayloadSerializer taskPayloadSerializer;

    /**
     * Instantiates the service
     *
     * @param standaloneTaskManager component responsible maintaining scheduled tasks.
     * @param taskPayloadSerializer
     */
    public EventSchedulerService(StandaloneTaskManager standaloneTaskManager,
                                 TaskPayloadSerializer taskPayloadSerializer) {
        this.standaloneTaskManager = standaloneTaskManager;
        this.taskPayloadSerializer = taskPayloadSerializer;
    }

    /**
     * Schedules the publication of an event
     *
     * @param request          contains the event and the timestamp on which to publish the event
     * @param responseObserver gets the taskId of the scheduled action
     */
    @Override
    public void scheduleEvent(ScheduleEventRequest request, StreamObserver<ScheduleToken> responseObserver) {
        doScheduleEvent(request.getEvent(), request.getInstant(), responseObserver);
    }

    /**
     * Cancel a scheduled event and schedule another in its place.
     *
     * @param request          request containing the new event, timestamp and optionally the token of the event to
     *                         cancel
     * @param responseObserver gets the taskId of the new scheduled action
     */
    @Override
    public void rescheduleEvent(RescheduleEventRequest request,
                                StreamObserver<ScheduleToken> responseObserver) {
        if (!request.getToken().equals("")) {
            standaloneTaskManager.cancel(request.getToken());
        }
        doScheduleEvent(request.getEvent(), request.getInstant(), responseObserver);
    }

    /**
     * Cancelled a scheduled request. If the request is already completed, this is a no-op.
     *
     * @param request          contains the token of the scheduled event to cancel
     * @param responseObserver gets acknowledgement of the cancellation
     */
    @Override
    public void cancelScheduledEvent(CancelScheduledEventRequest request,
                                     StreamObserver<InstructionAck> responseObserver) {
        try {
            standaloneTaskManager.cancel(request.getToken());
            responseObserver.onNext(InstructionAck.newBuilder()
                                                  .setSuccess(true)
                                                  .build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    private void doScheduleEvent(Event event, long instant, StreamObserver<ScheduleToken> responseObserver) {
        try {
            TaskPayload payload = taskPayloadSerializer.serialize(new ScheduledEventWrapper(
                    Topology.DEFAULT_CONTEXT, event.toByteArray()));
            String taskId = standaloneTaskManager.createTask(ScheduledEventExecutor.class.getName(),
                                                             payload,
                                                             instant);
            responseObserver.onNext(ScheduleToken.newBuilder()
                                                 .setToken(taskId)
                                                 .build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }
}
