/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.CancelScheduledeEventRequest;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class EventSchedulerService extends EventSchedulerGrpc.EventSchedulerImplBase implements
        AxonServerClientService {

    @Override
    public void scheduleEvent(ScheduleEventRequest request, StreamObserver<ScheduleToken> responseObserver) {
        responseObserver.onError(GrpcExceptionBuilder.build(ErrorCode.OTHER, "Not implemented in Axon Server SE yet"));
    }

    @Override
    public void rescheduleEvent(RescheduleEventRequest request, StreamObserver<InstructionAck> responseObserver) {
        responseObserver.onError(GrpcExceptionBuilder.build(ErrorCode.OTHER, "Not implemented in Axon Server SE yet"));
    }

    @Override
    public void cancelScheduledEvent(CancelScheduledeEventRequest request,
                                     StreamObserver<InstructionAck> responseObserver) {
        responseObserver.onError(GrpcExceptionBuilder.build(ErrorCode.OTHER, "Not implemented in Axon Server SE yet"));
    }
}
