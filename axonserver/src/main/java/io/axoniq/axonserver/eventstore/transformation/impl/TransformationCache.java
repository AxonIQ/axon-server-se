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

    public void init(String context) {
        eventStoreTransformationRepository.findByContext(context)
                .ifPresent(transformation -> {
                    EventStoreTransformation eventStoreTransformation = new EventStoreTransformation(transformation);
                    TransformationEntryStore store = transformationStoreRegistry.register(context,
                                                                                                            transformation.getTransformationId());
                    TransformEventsRequest entry = store.lastEntry();
                    if (entry != null) {
                        eventStoreTransformation.setPreviousToken(token(entry));
                    }
                    activeTransformations.put(transformation.getTransformationId(), eventStoreTransformation);
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
        activeTransformations.put(transformationId,  new EventStoreTransformation(transformationJpa));
    }

    @Transactional
    public void delete(String transformationId) {
        eventStoreTransformationRepository.deleteById(transformationId);
        activeTransformations.remove(transformationId);
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void setTransactionStatus(String transformationId, EventStoreTransformationJpa.Status status) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                      .orElseThrow(() -> new RuntimeException(
                                                                                              "Transformation not found"));
        transformationJpa.setStatus(status);
        eventStoreTransformationRepository.save(transformationJpa);
    }

    public Mono<Void> add(String transformationId, TransformEventsRequest transformEventsRequest) {
        EventStoreTransformation transformation = activeTransformations.get(transformationId);
        return transformationStoreRegistry.get(transformationId).append(transformEventsRequest)
                .doOnSuccess(result -> {
                    System.out.printf("Added entry #%d, token = %d%n", 0, token(transformEventsRequest));
                    transformation.setPreviousToken(token(transformEventsRequest));
                });
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
}
