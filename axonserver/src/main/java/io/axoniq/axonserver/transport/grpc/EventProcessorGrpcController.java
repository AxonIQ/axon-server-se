/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.Component;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.admin.AdminActionResult;
import io.axoniq.axonserver.grpc.admin.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc;
import io.axoniq.axonserver.grpc.admin.EventProcessorAdminServiceGrpc.EventProcessorAdminServiceImplBase;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.admin.LoadBalanceRequest;
import io.axoniq.axonserver.grpc.admin.LoadBalancingStrategy;
import io.axoniq.axonserver.grpc.admin.MoveSegment;
import io.axoniq.axonserver.grpc.admin.Result;
import io.axoniq.axonserver.transport.grpc.eventprocessor.EventProcessorIdMessage;
import io.axoniq.axonserver.transport.grpc.eventprocessor.EventProcessorMapping;
import io.axoniq.axonserver.util.StringUtils;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.function.Consumer;
import java.util.stream.StreamSupport;

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
    private final EventProcessorMapping eventProcessorMapping =new EventProcessorMapping();
    private final ContextProvider contextProvider;
    private final AxonServerAccessController axonServerAccessController;

    /**
     * Constructor that specify the service to perform the requested operation and the authentication provider.
     *
     * @param service                used to perform the requested operations
     * @param authenticationProvider used to retrieve the information related to the authenticated user
     */
    @Autowired
    public EventProcessorGrpcController(EventProcessorAdminService service,
                                        AuthenticationProvider authenticationProvider, ContextProvider contextProvider,
                                        AxonServerAccessController axonServerAccessController) {
        this.contextProvider = contextProvider;
        this.axonServerAccessController = axonServerAccessController;
        this.service = service;
        this.authenticationProvider = authenticationProvider;
    }

    /**
     * Processes the request to pause a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void pauseEventProcessor(EventProcessorIdentifier processorId,
                                    StreamObserver<AdminActionResult> responseObserver) {
        String contextName = determineContextName(processorId.getContextName());

        if (authenticationProvider.get().isAuthenticated() && !axonServerAccessController.allowed(
                EventProcessorAdminServiceGrpc.getPauseEventProcessorMethod().getFullMethodName(),
                contextName,
                authenticationProvider.get())) {
            returnAuthorizationError(responseObserver, "Pause operation not allowed");
            return;
        }

        service.pause(new EventProcessorIdMessage(contextName, processorId),
                      new GrpcAuthentication(authenticationProvider)).subscribe(result -> success(result,
                                                                                                  responseObserver),
                                                                                onError(responseObserver),
                                                                                responseObserver::onCompleted);
    }

    /**
     * Processes the request to start a specific event processor.
     *
     * @param eventProcessorId the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void startEventProcessor(EventProcessorIdentifier eventProcessorId,
                                    StreamObserver<AdminActionResult> responseObserver) {
        String contextName = determineContextName(eventProcessorId.getContextName());

        if (authenticationProvider.get().isAuthenticated() && !axonServerAccessController.allowed(
                EventProcessorAdminServiceGrpc.getStartEventProcessorMethod().getFullMethodName(),
                contextName,
                authenticationProvider.get())) {
            returnAuthorizationError(responseObserver, "Start operation not allowed");
            return;
        }

        service.start(new EventProcessorIdMessage(contextName, eventProcessorId),
                      new GrpcAuthentication(authenticationProvider)).subscribe(result -> success(result,
                                                                                                  responseObserver),
                                                                                onError(responseObserver),
                                                                                responseObserver::onCompleted);
    }

    /**
     * Processes the request to split the bigger segment of a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void splitEventProcessor(EventProcessorIdentifier processorId,
                                    StreamObserver<AdminActionResult> responseObserver) {
        String contextName = determineContextName(processorId.getContextName());

        if (authenticationProvider.get().isAuthenticated() && !axonServerAccessController.allowed(
                EventProcessorAdminServiceGrpc.getSplitEventProcessorMethod().getFullMethodName(),
                contextName,
                authenticationProvider.get())) {
            returnAuthorizationError(responseObserver, "Split operation not allowed");
            return;
        }

        service.split(new EventProcessorIdMessage(contextName, processorId),
                      new GrpcAuthentication(authenticationProvider)).subscribe(result -> success(result,
                                                                                                  responseObserver),
                                                                                onError(responseObserver),
                                                                                responseObserver::onCompleted);
    }

    /**
     * Processes the request to split the bigger segment of a specific event processor.
     *
     * @param processorId      the identifier of the event processor
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void mergeEventProcessor(EventProcessorIdentifier processorId,
                                    StreamObserver<AdminActionResult> responseObserver) {
        String contextName = determineContextName(processorId.getContextName());

        if (authenticationProvider.get().isAuthenticated() && !axonServerAccessController.allowed(
                EventProcessorAdminServiceGrpc.getMergeEventProcessorMethod().getFullMethodName(),
                contextName,
                authenticationProvider.get())) {
            returnAuthorizationError(responseObserver, "Merge operation not allowed");
            return;
        }

        service.merge(new EventProcessorIdMessage(contextName, processorId),
                      new GrpcAuthentication(authenticationProvider)).subscribe(result -> success(result,
                                                                                                  responseObserver),
                                                                                onError(responseObserver),
                                                                                responseObserver::onCompleted);
    }

    /**
     * Processes the request to move one segment of a certain event processor to a specific client.
     *
     * @param request          the request details
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void moveEventProcessorSegment(MoveSegment request, StreamObserver<AdminActionResult> responseObserver) {
        String contextName = determineContextName(request.getEventProcessor().getContextName());

        if (authenticationProvider.get().isAuthenticated() && !axonServerAccessController.allowed(
                EventProcessorAdminServiceGrpc.getMergeEventProcessorMethod().getFullMethodName(),
                contextName,
                authenticationProvider.get())) {
            returnAuthorizationError(responseObserver, "Move operation not allowed");
            return;
        }

        service.move(new EventProcessorIdMessage(contextName, request.getEventProcessor()),
                     request.getSegment(),
                     request.getTargetClientId(),
                     new GrpcAuthentication(authenticationProvider)).subscribe(result -> success(result,
                                                                                                 responseObserver),
                                                                               onError(responseObserver),
                                                                               responseObserver::onCompleted);
    }

    private void returnAuthorizationError(StreamObserver<?> responseObserver, String message) {
        responseObserver.onError(GrpcExceptionBuilder.build(new MessagingPlatformException(
                ErrorCode.AUTHENTICATION_INVALID_TOKEN, message)));
    }

    /**
     * Uses the responseObserver to return all event processors registered by all clients connected to on AS
     *
     * @param request          not used
     * @param responseObserver the grpc {@link StreamObserver} used to send the event processor stream
     */
    @Override
    public void getAllEventProcessors(Empty request, StreamObserver<EventProcessor> responseObserver) {
        service.eventProcessors(new GrpcAuthentication(authenticationProvider)).map(eventProcessorMapping).subscribe(
                responseObserver::onNext,
                onError(responseObserver),
                responseObserver::onCompleted);
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
        service.eventProcessorsByComponent(component, new GrpcAuthentication(authenticationProvider)).map(
                eventProcessorMapping).subscribe(responseObserver::onNext,
                                                 onError(responseObserver),
                                                 responseObserver::onCompleted);
    }

    /**
     * Balance the load for the specified event processor among the connected client.
     *
     * @param request          that contains processor name, token store identifier, chosen strategy
     * @param responseObserver the grpc {@link StreamObserver}
     */
    @Override
    public void loadBalanceProcessor(LoadBalanceRequest request, StreamObserver<Empty> responseObserver) {
        service.loadBalance(new EventProcessorIdMessage(contextProvider.getContext(), request.getProcessor()),
                            request.getStrategy(),
                            new GrpcAuthentication(authenticationProvider)).subscribe(unused -> {
        }, onError(responseObserver), responseObserver::onCompleted);
    }

    @Override
    public void setAutoLoadBalanceStrategy(LoadBalanceRequest request, StreamObserver<Empty> responseObserver) {
        service.setAutoLoadBalanceStrategy(new EventProcessorIdMessage(contextProvider.getContext(),
                                                                       request.getProcessor()),
                                           request.getStrategy(),
                                           new GrpcAuthentication(authenticationProvider)).subscribe(unused -> {
        }, onError(responseObserver), responseObserver::onCompleted);
    }

    @Override
    public void getBalancingStrategies(Empty request, StreamObserver<LoadBalancingStrategy> responseObserver) {
        StreamSupport.stream(service.getBalancingStrategies(new GrpcAuthentication(authenticationProvider))
                                    .spliterator(), false).map(str -> LoadBalancingStrategy.newBuilder()
                                                                                           .setStrategy(str.getName())
                                                                                           .setLabel(str.getLabel())
                                                                                           .build()).forEach(
                responseObserver::onNext);
        responseObserver.onCompleted();
    }

    private Consumer<Throwable> onError(StreamObserver<?> responseObserver) {
        return error -> responseObserver.onError(GrpcExceptionBuilder.build(error));
    }

    private void success(io.axoniq.axonserver.admin.eventprocessor.api.Result result,
                         StreamObserver<AdminActionResult> responseObserver) {
        responseObserver.onNext(AdminActionResult.newBuilder().setResult(map(result)).build());
    }

    private Result map(io.axoniq.axonserver.admin.eventprocessor.api.Result result) {
        if (result.isSuccess()) {
            return Result.SUCCESS;
        }
        if (result.isAccepted()) {
            return Result.ACCEPTED;
        }
        return Result.UNRECOGNIZED;
    }

    /**
     * Find the right context name to use for the operation
     *
     * @param requestContextName the context name supplied in the request
     * @return the context name from the request is available, if empty provides the context name from contextProvider
     */
    private String determineContextName(String requestContextName) {
        return StringUtils.isEmpty(requestContextName) ? contextProvider.getContext() : requestContextName;
    }
}
