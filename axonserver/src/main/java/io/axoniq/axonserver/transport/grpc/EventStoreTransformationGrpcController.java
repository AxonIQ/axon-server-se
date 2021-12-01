/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformEventRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

/**
 * GRPC endpoint for the event store transformation service.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class EventStoreTransformationGrpcController
        extends EventTransformationServiceGrpc.EventTransformationServiceImplBase
        implements AxonServerClientService {

    private static final Confirmation CONFIRMATION = Confirmation.newBuilder().setSuccess(true).build();

    private final ContextProvider contextProvider;
    private final AuthenticationProvider authenticationProvider;
    private final EventStoreTransformationService eventStoreTransformationService;

    public EventStoreTransformationGrpcController(ContextProvider contextProvider,
                                                  AuthenticationProvider authenticationProvider,
                                                  EventStoreTransformationService eventStoreTransformationService) {
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.eventStoreTransformationService = eventStoreTransformationService;
    }

    @Override
    public void startTransformation(StartTransformationRequest request,
                                    StreamObserver<TransformationId> responseObserver) {
        String context = contextProvider.getContext();
        eventStoreTransformationService.startTransformation(context,
                                                            request.getDescription(),
                                                            new GrpcAuthentication(authenticationProvider))
                                       .subscribe(id -> responseObserver.onNext(transformationId(id)),
                                                  throwable -> responseObserver.onError(GrpcExceptionBuilder.build(
                                                          throwable)),
                                                  responseObserver::onCompleted);
    }

    @Nonnull
    private TransformationId transformationId(String id) {
        return TransformationId.newBuilder()
                               .setId(id)
                               .build();
    }

    @Override
    public StreamObserver<TransformEventRequest> transformEvents(StreamObserver<Confirmation> responseObserver) {
        CallStreamObserver<Confirmation> serverCallStreamObserver =
                (CallStreamObserver<Confirmation>) responseObserver;
        serverCallStreamObserver.disableAutoInboundFlowControl();

        String context = contextProvider.getContext();
        return new StreamObserver<TransformEventRequest>() {
            final AtomicInteger pendingRequests = new AtomicInteger();
            final AtomicBoolean sendConfirmation = new AtomicBoolean();

            @Override
            public void onNext(TransformEventRequest request) {
                switch (request.getRequestCase()) {
                    case EVENT:
                        pendingRequests.incrementAndGet();
                        eventStoreTransformationService.replaceEvent(context,
                                                                     transformationId(request),
                                                                     request.getEvent().getToken(),
                                                                     request.getEvent().getEvent(),
                                                                     request.getPreviousToken(),
                                                                     new GrpcAuthentication(authenticationProvider))
                                                       .subscribe(r -> {
                                                                  },
                                                                  this::forwardError,
                                                                  this::handleRequestProcessed);
                        break;
                    case DELETE_EVENT:
                        pendingRequests.incrementAndGet();
                        eventStoreTransformationService.deleteEvent(context,
                                                                    transformationId(request),
                                                                    request.getDeleteEvent().getToken(),
                                                                    request.getPreviousToken(),
                                                                    new GrpcAuthentication(authenticationProvider))
                                                       .subscribe(r -> {
                                                                  },
                                                                  this::forwardError,
                                                                  this::handleRequestProcessed);
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }
            }

            private void handleRequestProcessed() {
                pendingRequests.decrementAndGet();
                checkRequestsDone();
                serverCallStreamObserver.request(1);
            }

            private void forwardError(Throwable throwable) {
                StreamObserverUtils.error(responseObserver, GrpcExceptionBuilder.build(throwable));
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
                sendConfirmation.set(true);
                checkRequestsDone();
            }

            private void checkRequestsDone() {
                if (pendingRequests.get() == 0 && sendConfirmation.compareAndSet(true, false)) {
                    try {
                        responseObserver.onNext(CONFIRMATION);
                        responseObserver.onCompleted();
                    } catch (Exception ex) {
                        // unable to send confirmation
                    }
                }
            }
        };
    }

    private String transformationId(TransformEventRequest request) {
        return request.hasTransformationId() ? request.getTransformationId().getId() : null;
    }


    @Override
    public void cancelTransformation(TransformationId request, StreamObserver<Confirmation> responseObserver) {
        String context = contextProvider.getContext();
        eventStoreTransformationService.cancelTransformation(context,
                                                             request.getId(),
                                                             new GrpcAuthentication(authenticationProvider))
                                       .subscribe(new VoidStreamObserverSubscriber<>(responseObserver, CONFIRMATION));
    }

    @Override
    public void applyTransformation(ApplyTransformationRequest request, StreamObserver<Confirmation> responseObserver) {
        String context = contextProvider.getContext();
        eventStoreTransformationService.applyTransformation(context,
                                                            request.getTransformationId().getId(),
                                                            request.getLastEventToken(),
                                                            request.getKeepOldVersions(),
                                                            new GrpcAuthentication(authenticationProvider))
                                       .subscribe(new VoidStreamObserverSubscriber<>(responseObserver, CONFIRMATION));
    }

    @Override
    public void rollbackTransformation(TransformationId request, StreamObserver<Confirmation> responseObserver) {
        String context = contextProvider.getContext();
        eventStoreTransformationService.rollbackTransformation(context,
                                                               request.getId(),
                                                               new GrpcAuthentication(authenticationProvider))
                                       .subscribe(new VoidStreamObserverSubscriber<>(responseObserver, CONFIRMATION));
    }

    @Override
    public void deleteOldVersions(TransformationId request, StreamObserver<Confirmation> responseObserver) {
        String context = contextProvider.getContext();
        eventStoreTransformationService.deleteOldVersions(context,
                                                          request.getId(),
                                                          new GrpcAuthentication(authenticationProvider))
                                       .subscribe(new VoidStreamObserverSubscriber<>(responseObserver, CONFIRMATION));
    }
}
