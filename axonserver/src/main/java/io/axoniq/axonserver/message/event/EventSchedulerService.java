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
import io.axoniq.axonserver.grpc.event.CancelScheduledeEventRequest;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import io.axoniq.axonserver.taskscheduler.LocalTaskManager;
import io.axoniq.axonserver.taskscheduler.Payload;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class EventSchedulerService extends EventSchedulerGrpc.EventSchedulerImplBase implements
        AxonServerClientService {

    private final LocalTaskManager localTaskManager;

    public EventSchedulerService(LocalTaskManager localTaskManager) {
        this.localTaskManager = localTaskManager;
    }

    @Override
    public void scheduleEvent(ScheduleEventRequest request, StreamObserver<ScheduleToken> responseObserver) {
        try {
            Payload payload = new Payload(Event.class.getName(), request.getEvent().toByteArray());
            String taskId = localTaskManager.createLocalTask(ScheduledEventExecutor.class.getName(),
                                                             payload,
                                                             request.getInstant());
            responseObserver.onNext(ScheduleToken.newBuilder()
                                                 .setToken(taskId)
                                                 .build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void rescheduleEvent(RescheduleEventRequest request, StreamObserver<InstructionAck> responseObserver) {
        try {
            localTaskManager.reschedule(request.getToken(), request.getInstant());
            responseObserver.onNext(InstructionAck.newBuilder()
                                                  .setSuccess(true)
                                                  .build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }

    @Override
    public void cancelScheduledEvent(CancelScheduledeEventRequest request,
                                     StreamObserver<InstructionAck> responseObserver) {
        try {
            localTaskManager.cancel(request.getToken());
            responseObserver.onNext(InstructionAck.newBuilder()
                                                  .setSuccess(true)
                                                  .build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            responseObserver.onError(GrpcExceptionBuilder.build(ex));
        }
    }
}
