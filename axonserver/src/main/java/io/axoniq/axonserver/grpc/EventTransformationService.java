/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import com.google.protobuf.Empty;
import io.axoniq.axonserver.config.AuthenticationProvider;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.EventTransformationServiceGrpc;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformationId;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.event.EventStoreTransformationService;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class EventTransformationService extends EventTransformationServiceGrpc.EventTransformationServiceImplBase
        implements AxonServerClientService {

    private final ContextProvider contextProvider;
    private final AuthenticationProvider authenticationProvider;
    private final Logger auditLog = AuditLog.getLogger();
    private final EventStoreTransformationService eventStoreTransformationService;

    public EventTransformationService(ContextProvider contextProvider,
                                      AuthenticationProvider authenticationProvider,
                                      EventStoreTransformationService eventStoreTransformationService) {
        this.contextProvider = contextProvider;
        this.authenticationProvider = authenticationProvider;
        this.eventStoreTransformationService = eventStoreTransformationService;
    }

    @Override
    public void startTransformation(Empty request, StreamObserver<TransformationId> responseObserver) {
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
        auditLog.info("{}@{}: Request to start transformation", authentication.getName(), context);
        eventStoreTransformationService.startTransformation(context)
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
    public StreamObserver<TransformEventsRequest> transformEvents(StreamObserver<Confirmation> responseObserver) {
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
//        auditLog.info("{}@{}: Request to start transformation", authentication.getName(), context);
        Sinks.Many<TransformEventsRequest> many = Sinks.many().unicast().onBackpressureBuffer();
        return new StreamObserver<TransformEventsRequest>() {
            final AtomicReference<Flux<TransformEventsRequest>> flux = new AtomicReference<>();

            @Override
            public void onNext(TransformEventsRequest transformEventsRequest) {
                if (flux.compareAndSet(null, many.asFlux())) {
                    eventStoreTransformationService.transformEvents(context, flux.get())
                                                   .subscribe(unused -> {
                                                              },
                                                              throwable -> responseObserver.onError(GrpcExceptionBuilder.build(
                                                                      throwable)),
                                                              () -> {
                                                                  responseObserver.onNext(confirmation());
                                                                  responseObserver.onCompleted();
                                                              });
                }
                many.emitNext(transformEventsRequest, ((signalType, emitResult) -> {
                    responseObserver.onError(GrpcExceptionBuilder.build(new RuntimeException(
                            "Emit error: " + emitResult)));
                    return false;
                }));
            }

            @Override
            public void onError(Throwable throwable) {
                many.tryEmitError(throwable);
            }

            @Override
            public void onCompleted() {
                many.tryEmitComplete();
            }
        };
    }

    private Confirmation confirmation() {
        return Confirmation.newBuilder().setSuccess(true).build();
    }

    @Override
    public void cancelTransformation(TransformationId request, StreamObserver<Confirmation> responseObserver) {
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
        auditLog.info("{}@{}: Request to cancel transformation {}", authentication.getName(), context, request.getId());
        eventStoreTransformationService.cancelTransformation(context, request.getId())
                                       .subscribe(id -> {
                                                  },
                                                  throwable -> responseObserver.onError(GrpcExceptionBuilder.build(
                                                          throwable)),
                                                  () -> {
                                                      responseObserver.onNext(confirmation());
                                                      responseObserver.onCompleted();
                                                  });
    }

    @Override
    public void applyTransformation(TransformationId request, StreamObserver<Confirmation> responseObserver) {
        String context = contextProvider.getContext();
        Authentication authentication = authenticationProvider.get();
        auditLog.info("{}@{}: Request to apply transformation {}", authentication.getName(), context, request.getId());
        eventStoreTransformationService.applyTransformation(context, request.getId())
                                       .subscribe(id -> {
                                                  },
                                                  throwable -> responseObserver.onError(GrpcExceptionBuilder.build(
                                                          throwable)),
                                                  () -> {
                                                      responseObserver.onNext(confirmation());
                                                      responseObserver.onCompleted();
                                                  });
    }
}
