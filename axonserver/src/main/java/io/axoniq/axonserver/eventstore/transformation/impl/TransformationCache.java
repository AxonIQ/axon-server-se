/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import io.axoniq.axonserver.grpc.event.TransformEventsRequest;
import io.axoniq.axonserver.localstorage.file.NewSegmentVersion;
import io.axoniq.axonserver.localstorage.file.TransformationProgress;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PostConstruct;
import javax.transaction.Transactional;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class TransformationCache {

    private final Map<String, EventStoreTransformation> activeTransformations = new ConcurrentHashMap<>();
    private final EventStoreTransformationRepository eventStoreTransformationRepository;
    private final TransformationStoreRegistry transformationStoreRegistry;

    public TransformationCache(
            EventStoreTransformationRepository eventStoreTransformationRepository,
            TransformationStoreRegistry transformationStoreRegistry) {
        this.eventStoreTransformationRepository = eventStoreTransformationRepository;
        this.transformationStoreRegistry = transformationStoreRegistry;
    }

    @PostConstruct
    public void init() {
        eventStoreTransformationRepository.findAll()
                .forEach(transformation -> {
                    if (isActive(transformation.getStatus())) {
                        EventStoreTransformation eventStoreTransformation = new EventStoreTransformation(transformation.getTransformationId(),
                                                                                                         transformation.getContext());
                        TransformationEntryStore store = transformationStoreRegistry.register(transformation.getContext(),
                                                                                              transformation.getTransformationId());
                        TransformEventsRequest entry = store.lastEntry();
                        if (entry != null) {
                            eventStoreTransformation.previousToken(token(entry));
                        }
                        activeTransformations.put(transformation.getTransformationId(), eventStoreTransformation);
                    }
                });
    }

    private long token(TransformEventsRequest request) {
        switch (request.getRequestCase()) {
            case EVENT:
                return request.getEvent().getToken();
            case DELETE_EVENT:
                return request.getDeleteEvent().getToken();
            default:
                throw new IllegalArgumentException("Request without token");
        }
    }


    public EventStoreTransformation get(String transformationId) {
        return activeTransformations.get(transformationId);
    }

    @Transactional
    public void create(String context, String transformationId) {
        EventStoreTransformationJpa transformationJpa = new EventStoreTransformationJpa(transformationId, context);
        eventStoreTransformationRepository.save(transformationJpa);
        transformationStoreRegistry.register(context, transformationId);
        activeTransformations.put(transformationId,  new EventStoreTransformation(transformationId, context));
    }

    private boolean isActive(EventStoreTransformationJpa.Status status) {
        switch (status) {
            case CREATED:
            case APPLYING:
                return true;
            default:
                return false;
        }
    }

    @Transactional
    public void delete(String transformationId) {
        eventStoreTransformationRepository.deleteById(transformationId);
        activeTransformations.remove(transformationId);
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void setTransformationStatus(String transformationId, EventStoreTransformationJpa.Status status) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                      .orElseThrow(() -> new RuntimeException(
                                                                                              "Transformation not found"));
        transformationJpa.setStatus(status);
        eventStoreTransformationRepository.save(transformationJpa);
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void startApply(String transformationId, boolean keepOldVersions) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                          .orElseThrow(() -> new RuntimeException(
                                                                                                  "Transformation not found"));
        transformationJpa.setStatus(EventStoreTransformationJpa.Status.APPLYING);
        transformationJpa.setKeepOldVersions(keepOldVersions);
        eventStoreTransformationRepository.save(transformationJpa);
    }

    public Mono<Void> add(String transformationId, TransformEventsRequest transformEventsRequest) {
        EventStoreTransformation transformation = activeTransformations.get(transformationId);
        return transformationStoreRegistry.get(transformationId).append(transformEventsRequest)
                .doOnSuccess(result -> transformation.previousToken(token(transformEventsRequest)));
    }


    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void setProgress(String transformationId, TransformationProgress transformationProgress) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                          .orElseThrow(() -> new RuntimeException(
                                                                                                  "Transformation not found"));
        transformationJpa.setNextToken(transformationProgress.nextToken());
        if (transformationProgress instanceof NewSegmentVersion) {
            NewSegmentVersion newSegmentVersion = (NewSegmentVersion)transformationProgress;
            transformationJpa.add(new EventStoreTransformationLogJpa(newSegmentVersion.segment(), newSegmentVersion.version()));
        }

        eventStoreTransformationRepository.save(transformationJpa);
    }

    public EventStoreTransformationJpa.Status status(String transformationId) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                          .orElseThrow(() -> new RuntimeException(
                                                                                                  "Transformation not found"));
        return transformationJpa.getStatus();
    }

    @Transactional
    public void complete(String transformationId) {
        activeTransformations.remove(transformationId);
        setTransformationStatus(transformationId, EventStoreTransformationJpa.Status.DONE);
    }

    @Transactional
    public void sync(String transformationId, String context, EventStoreTransformationJpa.Status status,
                     boolean keepOldVersions) {
        EventStoreTransformationJpa transformationJpa = new EventStoreTransformationJpa(transformationId, context);
        transformationJpa.setStatus(status);
        transformationJpa.setKeepOldVersions(keepOldVersions);
        eventStoreTransformationRepository.save(transformationJpa);
        transformationStoreRegistry.register(context, transformationId);
        activeTransformations.put(transformationId,  new EventStoreTransformation(transformationId, context));
    }
}
