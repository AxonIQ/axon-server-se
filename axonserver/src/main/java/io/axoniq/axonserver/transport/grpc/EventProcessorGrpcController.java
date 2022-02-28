/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.LoadBalanceStrategyType;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.Component;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceImplBase;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.admin.LoadBalanceRequest;
import io.axoniq.axonserver.grpc.admin.MoveSegment;
import io.axoniq.axonserver.transport.grpc.eventprocessor.EventProcessorIdMessage;
import io.axoniq.axonserver.transport.grpc.eventprocessor.EventProcessorMapping;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.function.Consumer;

/**
 * Exposed through GRPC the operations applicable to an Event Processor.
 *
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 * @since 4.6
 */
@Controller
public class EventProcessorGrpcController extends EventProcessorAdminServiceImplBase
        implements AxonServerClientService {

    private final EventProcessorAdminService service;
    private final AuthenticationProvider authenticationProvider;
    private final EventProcessorMapping eventProcessorMapping;

    /**
     * Constructor that specify the service to perform the requested operation and the authentication provider.
     *
     * @param service                used to perform the requested operations
     * @param authenticationProvider used to retrieve the information related to the authenticated user
     */
    @Autowired
    public EventProcessorGrpcController(EventProcessorAdminService service,
                                        AuthenticationProvider authenticationProvider) {
        this(service, authenticationProvider, new EventProcessorMapping());
    }

    public EventProcessorGrpcController(EventProcessorAdminService service,
                                        AuthenticationProvider authenticationProvider,
                                        EventProcessorMapping eventProcessorMapping) {
        this.service = service;
        this.authenticationProvider = authenticationProvider;
        this.eventProcessorMapping = eventProcessorMapping;
    }

    /**
     * Processes the request to pause a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void pauseEventProcessor(EventProcessorIdentifier processorId, StreamObserver<Empty> responseObserver) {
        service.pause(new EventProcessorIdMessage(processorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, onError(responseObserver), responseObserver::onCompleted);
    }

    /**
     * Processes the request to start a specific event processor.
     *
     * @param eventProcessorId the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void startEventProcessor(EventProcessorIdentifier eventProcessorId, StreamObserver<Empty> responseObserver) {
        service.start(new EventProcessorIdMessage(eventProcessorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, onError(responseObserver), responseObserver::onCompleted);
    }

    /**
     * Processes the request to split the bigger segment of a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void splitEventProcessor(EventProcessorIdentifier processorId, StreamObserver<Empty> responseObserver) {
        service.split(new EventProcessorIdMessage(processorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, onError(responseObserver), responseObserver::onCompleted);
    }

    /**
     * Processes the request to split the bigger segment of a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void mergeEventProcessor(EventProcessorIdentifier processorId, StreamObserver<Empty> responseObserver) {
        service.merge(new EventProcessorIdMessage(processorId), new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, onError(responseObserver), responseObserver::onCompleted);
    }

    /**
     * Processes the request to move one segment of a certain event processor to a specific client.
     *
     * @param request          the request details
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void moveEventProcessorSegment(MoveSegment request, StreamObserver<Empty> responseObserver) {
        service.move(new EventProcessorIdMessage(request.getEventProcessor()),
                     request.getSegment(),
                     request.getTargetClientId(),
                     new GrpcAuthentication(authenticationProvider))
               .subscribe(unused -> {}, onError(responseObserver), responseObserver::onCompleted);
    }

    /**
     * Uses the responseObserver to return all event processors registered by all clients connected to on AS
     *
     * @param request          not used
     * @param responseObserver the grpc {@link StreamObserver} used to send the event processor stream
     */
    @Override
    public void getAllEventProcessors(Empty request, StreamObserver<EventProcessor> responseObserver) {
        service.eventProcessors(new GrpcAuthentication(authenticationProvider))
               .map(eventProcessorMapping)
               .subscribe(responseObserver::onNext, onError(responseObserver), responseObserver::onCompleted);
    }

    /**
     * Uses the responseObserver to return the event processors registered by a specific application connected to on AS
     *
     * @param request          the request contains the application name used to filter the event processors
     * @param responseObserver the grpc {@link StreamObserver} used to send the event processor stream
     */
    @Override
    public void getEventProcessorsByComponent(Component request, StreamObserver<EventProcessor> responseObserver) {
        String component = request.getComponent();
        service.eventProcessorsByComponent(component, new GrpcAuthentication(authenticationProvider))
               .map(eventProcessorMapping)
               .subscribe(responseObserver::onNext, onError(responseObserver), responseObserver::onCompleted);
    }

    /**
     * Balance the load for the specified event processor among the connected client.
     *
     * @param request            that contains processor name, token store identifier, chosen strategy
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void loadBalanceProcessor(LoadBalanceRequest request, StreamObserver<Empty> responseObserver) {
        LoadBalanceStrategyType strategy = null;
        switch (request.getStrategy()) {
            case DEFAULT:
                strategy = LoadBalanceStrategyType.DEFAULT;
                break;
            case THREAD_NUMBER:
                strategy =  LoadBalanceStrategyType.THREAD_NUMBER;
                break;
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unknown load balancing strategy: " + request.getStrategy());
        }

        service.loadBalance(request.getProcessor().getProcessorName(), request.getProcessor().getTokenStoreIdentifier(), strategy, new GrpcAuthentication(authenticationProvider))
                .subscribe(unused -> {
                }, onError(responseObserver), responseObserver::onCompleted);
    }

    private Consumer<Throwable> onError(StreamObserver<?> responseObserver) {
        return error -> responseObserver.onError(GrpcExceptionBuilder.build(error));
    }
}
