/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.apply;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.eventstore.transformation.ReplaceEvent;
import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntry;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStoreProvider;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultTransformationApplyExecutor implements TransformationApplyExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTransformationApplyExecutor.class);
    private static final Event EMPTY_EVENT = Event.getDefaultInstance();

    private final TransformationEntryStoreProvider transformationEntryStoreSupplier;
    private final TransformationProgressStore localStateStore;
    private final EventStoreTransformer transformer;
    private final Set<String> applyingTransformations = new CopyOnWriteArraySet<>();

    public DefaultTransformationApplyExecutor(TransformationEntryStoreProvider transformationEntryStoreSupplier,
                                              TransformationProgressStore localStateStore,
                                              EventStoreTransformer transformer) {
        this.transformationEntryStoreSupplier = transformationEntryStoreSupplier;
        this.localStateStore = localStateStore;
        this.transformer = transformer;
    }

    @Override
    public Mono<Void> apply(Transformation transformation) {
        Flux<EventWithToken> transformedEvents =
                localStateStore.stateFor(transformation.id())
                               .switchIfEmpty(localStateStore.initState(transformation.id()))
                               .map(state -> state.lastAppliedSequence() + 1)
                               .flatMapMany(firstSequence -> transformationEntryStoreSupplier.provide(transformation.context(),
                                                                                                      transformation.id())
                                                                                             .flatMapMany(store -> store.readClosed(
                                                                                                     firstSequence,
                                                                                                     transformation.lastSequence())))
                               .map(TransformationEntry::payload)
                               .flatMapSequential(this::parseFrom)
                               .map(this::eventWithToken);
        return Mono.fromSupplier(() -> applyingTransformations.add(transformation.id()))
                   .filter(inactive -> inactive)
                   .switchIfEmpty(Mono.error(new RuntimeException("applying already in progress")))
                   .then(transformer.transformEvents(transformation.context(),
                                                     transformation.version(),
                                                     transformedEvents)
                                    .concatMap(progress -> localStateStore.incrementLastSequence(transformation.id(),
                                                                                                 progress))
                                    .then(localStateStore.markAsApplied(transformation.id()))
                                    .doFinally(onFinally -> applyingTransformations.remove(transformation.id())))
                   .doFirst(() -> logger.info("Starting to apply transformation {} to context {}",
                                              transformation.id(),
                                              transformation.context()))
                   .doOnSuccess(v -> logger.info("Transformation {} applied successfully to local store.",
                                                 transformation.id()))
                   .doOnError(t -> logger.warn("Failed to apply to local store the transformation {}", transformation));
    }

    private EventWithToken eventWithToken(TransformationAction transformationAction) {
        if (transformationAction.hasDeleteEvent()) {
            long token = transformationAction.getDeleteEvent()
                                             .getToken();
            return eventWithToken(EMPTY_EVENT, token);
        } else {
            ReplaceEvent replaceEvent = transformationAction.getReplaceEvent();
            Event event = replaceEvent.getEvent();
            long token = replaceEvent.getToken();
            return eventWithToken(event, token);
        }
    }

    private EventWithToken eventWithToken(Event event, long token) {
        return EventWithToken.newBuilder()
                             .setEvent(event)
                             .setToken(token)
                             .build();
    }

    private Mono<TransformationAction> parseFrom(
            byte[] data) { //TODO parse is used also in TransformationActionsSnapshotDataStore - extract it
        return Mono.create(sink -> {
            try {
                sink.success(TransformationAction.parseFrom(data));
            } catch (InvalidProtocolBufferException e) {
                sink.error(e);
            }
        });
    }
}