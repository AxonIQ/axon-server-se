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
import io.axoniq.axonserver.localstorage.file.TransformationProgress;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final EventStoreTransformationProgressRepository eventStoreTransformationProgressRepository;


    public TransformationCache(
            EventStoreTransformationRepository eventStoreTransformationRepository,
            TransformationStoreRegistry transformationStoreRegistry,
            EventStoreTransformationProgressRepository eventStoreTransformationProgressRepository) {
        this.eventStoreTransformationRepository = eventStoreTransformationRepository;
        this.transformationStoreRegistry = transformationStoreRegistry;
        this.eventStoreTransformationProgressRepository = eventStoreTransformationProgressRepository;
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
    public void create(String context, String transformationId, int version, String description) {
        synchronized (eventStoreTransformationRepository) {
            EventStoreTransformationJpa transformationJpa = new EventStoreTransformationJpa(transformationId, context);
            transformationJpa.setVersion(version);
            transformationJpa.setDescription(description);
            eventStoreTransformationRepository.save(transformationJpa);
            transformationStoreRegistry.register(context, transformationId);
            activeTransformations.put(transformationId, new EventStoreTransformation(transformationId, context));
        }
    }

    public void reserve(String context, String transformationId) {
        synchronized (activeTransformations) {
            if (activeTransformations.values().stream().anyMatch(tr -> context.equals(tr.context()))) {
                throw new RuntimeException("Transformation already active for " + context);
            }
            activeTransformations.put(transformationId, new EventStoreTransformation(transformationId, context));
        }
    }

    private boolean isActive(EventStoreTransformationJpa.Status status) {
        return !status.equals(EventStoreTransformationJpa.Status.DONE);
    }

    @Transactional
    public void delete(String transformationId) {
        eventStoreTransformationRepository.deleteById(transformationId);
        eventStoreTransformationProgressRepository.findById(transformationId)
                                                          .ifPresent(eventStoreTransformationProgressRepository::delete);
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
    public void startApply(String transformationId, boolean keepOldVersions, String appliedBy,
                           Date appliedDate) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                          .orElseThrow(() -> new RuntimeException(
                                                                                                  "Transformation not found"));
        transformationJpa.setStatus(EventStoreTransformationJpa.Status.CLOSED);
        transformationJpa.setDateApplied(appliedDate);
        transformationJpa.setAppliedBy(appliedBy);
        transformationJpa.setKeepOldVersions(keepOldVersions);
        eventStoreTransformationRepository.save(transformationJpa);
        getOrCreateProgress(transformationId);
   }

    public Mono<Void> add(String transformationId, TransformEventsRequest transformEventsRequest) {
        EventStoreTransformation transformation = activeTransformations.get(transformationId);
        return transformationStoreRegistry.get(transformationId).append(transformEventsRequest)
                .doOnSuccess(result -> transformation.previousToken(token(transformEventsRequest)));
    }


    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public void setProgress(String transformationId, TransformationProgress transformationProgress) {
        EventStoreTransformationProgress transformationJpa = eventStoreTransformationProgressRepository.findById(transformationId)
                                                                                          .orElseThrow(() -> new RuntimeException(
                                                                                                  "Transformation not found"));
        transformationJpa.setLastTokenApplied(transformationProgress.lastTokenProcessed());
        eventStoreTransformationProgressRepository.save(transformationJpa);
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
                     boolean keepOldVersions,
                     int version,
                     String description,
                     String appliedBy,
                     Date dateApplied) {
        EventStoreTransformationJpa transformationJpa = new EventStoreTransformationJpa(transformationId, context);
        transformationJpa.setStatus(status);
        transformationJpa.setKeepOldVersions(keepOldVersions);
        transformationJpa.setVersion(version);
        transformationJpa.setDescription(description);
        transformationJpa.setAppliedBy(appliedBy);
        transformationJpa.setDateApplied(dateApplied);
        eventStoreTransformationRepository.save(transformationJpa);
        if (!EventStoreTransformationJpa.Status.DONE.equals(status)) {
            transformationStoreRegistry.register(context, transformationId);
            activeTransformations.put(transformationId,  new EventStoreTransformation(transformationId, context));
        }
    }

    public List<EventStoreTransformationJpa> findTransformation(String context) {
        return eventStoreTransformationRepository.findByContext(context);
    }

    public Optional<EventStoreTransformationJpa> transformation(String transformationId) {
        return eventStoreTransformationRepository.findById(transformationId);
    }

    public EventStoreTransformationProgress getOrCreateProgress(String transformationId) {
        return eventStoreTransformationProgressRepository.findById(transformationId)
                .orElseGet(() -> {
                    EventStoreTransformationProgress progress = new EventStoreTransformationProgress();
                    progress.setTransformationId(transformationId);
                    return eventStoreTransformationProgressRepository.save(progress);
                });
    }

    public Optional<EventStoreTransformationProgress> progress(String transformationId) {
        return eventStoreTransformationProgressRepository.findById(transformationId);
    }


    public void completeTransformation(String transformationId) {
        EventStoreTransformationProgress transformation = getOrCreateProgress(transformationId);
        transformation.setCompleted(true);
        eventStoreTransformationProgressRepository.save(transformation);
    }

    public int nextVersion(String context) {
        AtomicInteger lastVersion = new AtomicInteger(0);
        findTransformation(context).forEach(t -> {
            lastVersion.updateAndGet(old -> Math.max(old, t.getVersion()));
        });
        return lastVersion.get()+1;
    }
}
