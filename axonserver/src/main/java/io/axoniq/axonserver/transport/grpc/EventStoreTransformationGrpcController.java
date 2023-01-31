/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.event.ApplyTransformationRequest;
import io.axoniq.axonserver.grpc.event.CompactionRequest;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.StartTransformationRequest;
import io.axoniq.axonserver.grpc.event.TransformRequest;
import io.axoniq.axonserver.grpc.event.TransformRequestAck;
import io.axoniq.axonserver.grpc.event.Transformation;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.grpc.event.TransformationState;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
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

    private final Logger logger = LoggerFactory.getLogger(EventStoreTransformationGrpcController.class);

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
        String uuid = UUID.randomUUID().toString();
        eventStoreTransformationService.start(uuid,
                                              context,
                                              request.getDescription(),
                                              new GrpcAuthentication(authenticationProvider))
                                       .doOnSuccess(v -> logger.info("Transformation Created with id {}", uuid))
                                       .subscribe(v -> {},
                                                  throwable -> responseObserver.onError(GrpcExceptionBuilder.build(
                                                          throwable)),
                                                  () -> {
                                                      responseObserver.onNext(transformationId(uuid));
                                                      responseObserver.onCompleted();
                                                  });
    }

    @Nonnull
    private TransformationId transformationId(String id) {
        return TransformationId.newBuilder()
                               .setId(id)
                               .build();
    }

    @Override
    public StreamObserver<TransformRequest> transformEvents(StreamObserver<TransformRequestAck> responseObserver) {
        CallStreamObserver<TransformRequestAck> serverCallStreamObserver =
                (CallStreamObserver<TransformRequestAck>) responseObserver;
        serverCallStreamObserver.disableAutoInboundFlowControl();

        String context = contextProvider.getContext();
        return new StreamObserver<>() {
            final AtomicInteger pendingRequests = new AtomicInteger();
            final AtomicBoolean sendConfirmation = new AtomicBoolean();

            @Override
            public void onNext(TransformRequest request) {
                switch (request.getRequestCase()) {
                    case EVENT:
                        pendingRequests.incrementAndGet();
                        eventStoreTransformationService.replaceEvent(context,
                                                                     transformationId(request),
                                                                     request.getEvent().getToken(),
                                                                     request.getEvent().getEvent(),
                                                                     request.getSequence(),
                                                                     new GrpcAuthentication(authenticationProvider))
                                                       .subscribe(r -> {
                                                                  },
                                                                  this::forwardError,
                                                                  () -> {
                                                                      responseObserver.onNext(ack(request.getSequence()));
                                                                      handleRequestProcessed();
                                                                  });
                        break;
                    case DELETE_EVENT:
                        pendingRequests.incrementAndGet();
                        eventStoreTransformationService.deleteEvent(context,
                                                                    transformationId(request),
                                                                    request.getDeleteEvent().getToken(),
                                                                    request.getSequence(),
                                                                    new GrpcAuthentication(authenticationProvider))
                                                       .timeout(Duration.ofMinutes(1))
                                                       .subscribe(r -> {
                                                                  },
                                                                  this::forwardError,
                                                                  () -> {
                                                                      responseObserver.onNext(ack(request.getSequence()));
                                                                      handleRequestProcessed();
                                                                  });
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
                        responseObserver.onCompleted();
                    } catch (Exception ex) {
                        // unable to send confirmation
                    }
                }
            }
        };
    }

    private TransformRequestAck ack(long sequence) {
        return TransformRequestAck.newBuilder()
                                  .setSequence(sequence)
                                  .build();
    }

    private String transformationId(TransformRequest request) {
        return request.hasTransformationId() ? request.getTransformationId().getId() : null;
    }


    @Override
    public void cancelTransformation(TransformationId request, StreamObserver<Empty> responseObserver) {
        String context = contextProvider.getContext();
        eventStoreTransformationService.cancel(context,
                                               request.getId(),
                                               new GrpcAuthentication(authenticationProvider))
                                       .subscribe(new VoidStreamObserverSubscriber(responseObserver));
    }

    @Override
    public void applyTransformation(ApplyTransformationRequest request, StreamObserver<Empty> responseObserver) {
        String context = contextProvider.getContext();
        eventStoreTransformationService.startApplying(context,
                                                      request.getTransformationId().getId(),
                                                      request.getLastSequence(),
                                                      new GrpcAuthentication(authenticationProvider))
                                       .subscribe(new VoidStreamObserverSubscriber(responseObserver));
    }


    @Override
    public void compact(CompactionRequest request, StreamObserver<Empty> responseObserver) {
        String context = contextProvider.getContext();
        String uuid = UUID.randomUUID().toString(); //TODO return uuid
        eventStoreTransformationService.compact(uuid,
                                                context,
                                                new GrpcAuthentication(authenticationProvider))
                                       .subscribe(new VoidStreamObserverSubscriber(responseObserver));
    }


    @Override
    public void transformations(Empty request, StreamObserver<Transformation> responseObserver) {
        eventStoreTransformationService.transformations(contextProvider.getContext(),
                                                        new GrpcAuthentication(authenticationProvider))
                                       .map(transformation -> {
                                           String description = Objects.toString(transformation.description(), "");
                                           String context = Objects.toString(transformation.context());
                                           return Transformation.newBuilder()
                                                                .setTransformationId(TransformationId.newBuilder()
                                                                                                     .setId(transformation.id()))
                                                                .setDescription(description)
                                                                .setContext(context)
                                                                .setState(statusMapping.get(transformation.status()))
                                                                .setSequence(transformation.lastSequence()
                                                                                           .orElse(-1L))
                                                                .build();
                                       })
                                       .subscribe(responseObserver::onNext,
                                                  responseObserver::onError,
                                                  responseObserver::onCompleted);
    }


    private final Map<EventStoreTransformationService.Transformation.Status, TransformationState> statusMapping
            = new EnumMap<EventStoreTransformationService.Transformation.Status, TransformationState>(
            EventStoreTransformationService.Transformation.Status.class) {{
        this.put(EventStoreTransformationService.Transformation.Status.ACTIVE, TransformationState.ACTIVE);
        this.put(EventStoreTransformationService.Transformation.Status.APPLYING, TransformationState.APPLYING);
        this.put(EventStoreTransformationService.Transformation.Status.APPLIED, TransformationState.APPLIED);
        this.put(EventStoreTransformationService.Transformation.Status.CANCELLED, TransformationState.CANCELLED);
    }};
}
