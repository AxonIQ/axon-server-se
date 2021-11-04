/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import io.axoniq.axonserver.grpc.event.DeletedEvent;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.grpc.event.TransformedEvent;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.TransformationProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.Math.max;

/**
 * Responsible for executing the event store transformation actions.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class TransformationProcessor {

    private final Logger logger = LoggerFactory.getLogger(TransformationProcessor.class);
    private final TransformationStateManager transformationStateManager;
    private final ApplicationContext applicationContext;

    public TransformationProcessor(
            ApplicationContext applicationContext,
            TransformationStateManager transformationStateManager) {
        this.applicationContext = applicationContext;
        this.transformationStateManager = transformationStateManager;
    }

    /**
     * Starts a new transformation
     * @param context the context in which to transform the events
     * @param transformationId a unique transformation id
     * @param version the version of the event store after this transformation
     * @param description a description for the transformation
     */
    public void startTransformation(String context, String transformationId, int version, String description) {
        transformationStateManager.create(context, transformationId, version, description);
    }

    public Mono<Void> deleteEvent(String transformationId, long token) {
        return transformationStateManager.add(transformationId, deleteEventEntry(token));
    }

    public Mono<Void> replaceEvent(String transformationId, long token, Event event) {
        return transformationStateManager.add(transformationId, replaceEventEntry(token, event));
    }

    public void cancel(String transformationId) {
        transformationStateManager.delete(transformationId);
    }

    public void complete(String transformationId) {
        transformationStateManager.complete(transformationId);
    }

    /**
     * Starts applying a transformation. The transformation runs asynchronously, the operation competes the future
     * when the apply process is completed.
     * @param transformationId the transformation id
     * @param keepOldVersions flag to indicate if the apply process should keep old versions of modified events
     * @param appliedBy the name of the application/user starting the apply process
     * @param appliedDate the time the apply process was started
     * @param firstEventToken token of the first transformed event in the transformation
     * @param lastEventToken token of the last transformed event in the transformation
     * @return a completable future
     */
    public CompletableFuture<Void> apply(String transformationId, boolean keepOldVersions, String appliedBy,
                                         Date appliedDate, long firstEventToken, long lastEventToken) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            TransformationEntryStore transformationFileStore = transformationStateManager.entryStore(transformationId);
            EventStoreTransformationJpa transformation = transformationStateManager.transformation(transformationId)
                                                                                   .orElseThrow(() -> new RuntimeException(
                                                                                           "Transformation not found"));
            transformationStateManager.startApply(transformationId,
                                                  keepOldVersions,
                                                  appliedBy,
                                                  appliedDate,
                                                  firstEventToken,
                                                  lastEventToken);
            return doApply(transformationFileStore,
                           transformation.getContext(),
                           transformationId,
                           keepOldVersions,
                           transformation.getVersion(),
                           firstEventToken,
                           lastEventToken,
                           0);
        } catch (Exception ex) {
            future.completeExceptionally(ex);
        }
        return future;
    }

    private CompletableFuture<Void> doApply(TransformationEntryStore transformationFileStore, String context,
                                            String transformationId, boolean keepOldVersions, int version,
                                            long firstEventToken,
                                            long lastEventToken,
                                            long nextToken) {

        if (lastEventToken < 0) {
            transformationStateManager.completeProgress(transformationId);
            return CompletableFuture.completedFuture(null);
        }
        long firstToken = max(firstEventToken, nextToken);

        logger.info("{}: Start apply transformation from {} to {}", context, firstToken, lastEventToken);
        try (CloseableIterator<TransformEventsRequest> iterator = transformationFileStore.iterator()) {
            if (iterator.hasNext()) {
                TransformEventsRequest transformationEntry = iterator.next();
                AtomicReference<TransformEventsRequest> request = new AtomicReference<>(transformationEntry);
                logger.debug("Next token {}", token(request.get()));
                return localEventStore().transformEvents(context,
                                                         firstToken,
                                                         lastEventToken,
                                                         keepOldVersions,
                                                         version,
                                                         (event, token) -> processEvent(iterator,
                                                                                        request,
                                                                                        event,
                                                                                        token),
                                                         transformationProgress -> handleTransformationProgress(context,
                                                                                                                transformationId,
                                                                                                                transformationProgress))
                                        .thenAccept(r -> {
                                            iterator.close();
                                            transformationStateManager.completeProgress(transformationId);
                                        });
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private Event processEvent(CloseableIterator<TransformEventsRequest> iterator,
                           AtomicReference<TransformEventsRequest> request, Event event, Long token) {
        logger.debug("Found token {}", token);
        Event result = event;
        TransformEventsRequest nextRequest = request.get();
        long t = token(nextRequest);
        while (t < token && iterator.hasNext()) {
            nextRequest = iterator.next();
            request.set(nextRequest);
            t = token(nextRequest);
        }

        if (token(nextRequest) == token) {
            result = applyTransformation(event, nextRequest);
            if (iterator.hasNext()) {
                request.set(iterator.next());
                logger.debug("Next token {}", token(request.get()));
            }
        }
        return result;
    }

    private void handleTransformationProgress(String context, String transformationId,
                                              TransformationProgress transformationProgress) {
        logger.info("{}: Transformation {} Progress {}", context, transformationId, transformationProgress);
        transformationStateManager.setProgress(transformationId, transformationProgress);
    }

    private TransformEventsRequest deleteEventEntry(long token) {
        return TransformEventsRequest.newBuilder()
                                     .setDeleteEvent(DeletedEvent.newBuilder()
                                                                 .setToken(token))
                                     .build();
    }

    private TransformEventsRequest replaceEventEntry(long token, Event event) {
        return TransformEventsRequest.newBuilder()
                                     .setEvent(TransformedEvent.newBuilder()
                                                               .setToken(token)
                                                               .setEvent(event))
                                     .build();
    }


    private Event applyTransformation(Event original, TransformEventsRequest transformRequest) {
        switch (transformRequest.getRequestCase()) {
            case EVENT:
                return merge(original, transformRequest.getEvent().getEvent());
            case DELETE_EVENT:
                return nullify(original);
            case REQUEST_NOT_SET:
                break;
        }
        return original;
    }

    private Event merge(Event original, Event updated) {
        return Event.newBuilder(original)
                    .clearMetaData()
                    .setAggregateType(updated.getAggregateType())
                    .setPayload(updated.getPayload())
                    .putAllMetaData(updated.getMetaDataMap())
                    .build();
    }

    private Event nullify(Event event) {
        return Event.newBuilder(event)
                    .clearPayload()
                    .clearMessageIdentifier()
                    .clearMetaData()
                    .build();
    }

    private long token(TransformEventsRequest nextRequest) {
        switch (nextRequest.getRequestCase()) {
            case EVENT:
                return nextRequest.getEvent().getToken();
            case DELETE_EVENT:
                return nextRequest.getDeleteEvent().getToken();
            default:
                throw new IllegalArgumentException("Request without token");
        }
    }

    /**
     * Restart a previously aborted transformation for a context.
     * @param context the name of the context
     * @return completable future with the transformation id of the applied transformation
     */
    public CompletableFuture<String> restartApply(String context) {
        CompletableFuture<String> future = new CompletableFuture<>();
        List<EventStoreTransformationJpa> toApply = transformationStateManager.findTransformations(context)
                                                                              .stream()
                                                                              .filter(transformation -> EventStoreTransformationJpa.Status.CLOSED.equals(
                                                                                      transformation.getStatus()))
                                                                              .collect(Collectors.toList());
        if (toApply.isEmpty()) {
            future.complete(null);
        } else if (toApply.size() > 1) {
            logger.warn("{}: {} transformations to apply", context, toApply.size());
            future.complete(null);
        } else {
            EventStoreTransformationJpa transformation = toApply.get(0);
            Optional<EventStoreTransformationProgressJpa> progress = transformationStateManager.progress(transformation.getTransformationId());
            if (!progress.isPresent()) {
                future.complete(null);
            } else {
                TransformationEntryStore transformationFileStore = transformationStateManager.entryStore(
                        transformation.getTransformationId());
                transformationStateManager.reserve(context, transformation.getTransformationId());
                return doApply(transformationFileStore,
                               transformation.getContext(),
                               transformation.getTransformationId(),
                               transformation.isKeepOldVersions(),
                               transformation.getVersion(),
                               transformation.getFirstEventToken(),
                               transformation.getLastEventToken(),
                               progress.get().getLastTokenApplied()+1)
                        .thenApply(v -> transformation.getTransformationId());
            }
        }
        return future;
    }

    public void deleteOldVersions(String context, String id) {
        transformationStateManager.transformation(id)
                                  .ifPresent(transformation -> localEventStore().deleteOldVersions(context,
                                                                                                   transformation.getVersion()));
    }

    public void rollbackTransformation(String context, String transformationId) {
        transformationStateManager.transformation(transformationId)
                                  .ifPresent(transformation -> {
                                      localEventStore().rollbackSegments(context,
                                                                         transformation.getVersion());
                                      transformationStateManager.delete(transformationId);
                                  });
    }

    private LocalEventStore localEventStore() {
        return applicationContext.getBean(LocalEventStore.class);
    }
}
