/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import io.axoniq.axonserver.grpc.event.TransformEventRequest;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.localstorage.file.TransformationProgress;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;

/**
 * Manages the state of transactions. Updates state information in the control db and caches information about active
 * transactions for validation purposes.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
@Component
public class TransformationStateManager {

    private final Map<String, ActiveEventStoreTransformation> activeTransformations = new ConcurrentHashMap<>();
    private final EventStoreTransformationRepository eventStoreTransformationRepository;
    private final TransformationStoreRegistry transformationStoreRegistry;
    private final EventStoreTransformationProgressRepository eventStoreTransformationProgressRepository;
    private Set<String> applyingTransformations = new CopyOnWriteArraySet<>();


    public TransformationStateManager(
            EventStoreTransformationRepository eventStoreTransformationRepository,
            TransformationStoreRegistry transformationStoreRegistry,
            EventStoreTransformationProgressRepository eventStoreTransformationProgressRepository) {
        this.eventStoreTransformationRepository = eventStoreTransformationRepository;
        this.transformationStoreRegistry = transformationStoreRegistry;
        this.eventStoreTransformationProgressRepository = eventStoreTransformationProgressRepository;
    }

    /**
     * Loads information about active transformations at the start of Axon Server.
     */
    @PostConstruct
    public void init() {
        eventStoreTransformationRepository.findAll()
                                          .forEach(transformation -> {
                                              if (isActive(transformation.getStatus())) {
                                                  TransformationEntryStore store = transformationStoreRegistry.register(
                                                          transformation.getContext(),
                                                          transformation.getTransformationId());
                                                  long lastToken = -1;
                                                  TransformEventRequest entry = store.lastEntry();
                                                  if (entry != null) {
                                                      lastToken = token(entry);
                                                  }
                                                  activeTransformations.put(transformation.getTransformationId(),
                                                                            new ActiveEventStoreTransformation(
                                                                                    transformation.getTransformationId(),
                                                                                    transformation.getContext(),
                                                                                    transformation.getStatus(),
                                                                                    lastToken));
                                              }
                                          });
    }

    private long token(TransformEventRequest request) {
        switch (request.getRequestCase()) {
            case EVENT:
                return request.getEvent().getToken();
            case DELETE_EVENT:
                return request.getDeleteEvent().getToken();
            default:
                throw new IllegalArgumentException("Request without token");
        }
    }


    /**
     * Retrieves information on a transformation
     *
     * @param transformationId the identifier of the transformation
     * @return information on an active transformation or null
     */
    public ActiveEventStoreTransformation get(String transformationId) {
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
            activeTransformations.put(transformationId, new ActiveEventStoreTransformation(transformationId,
                                                                                           context,
                                                                                           transformationJpa.getStatus(),
                                                                                           -1));
        }
    }

    public void reserve(String context, String transformationId) {
        synchronized (activeTransformations) {
            if (activeTransformations.values().stream().anyMatch(tr -> context.equals(tr.context()))) {
                throw new RuntimeException("Transformation already active for " + context);
            }
            activeTransformations.put(transformationId,
                                      new ActiveEventStoreTransformation(transformationId,
                                                                         context, null, -1));
        }
    }

    private boolean isActive(EventStoreTransformationJpa.Status status) {
        return !status.equals(EventStoreTransformationJpa.Status.DONE);
    }

    @Transactional
    public void delete(String transformationId) {
        eventStoreTransformationRepository.findById(transformationId)
                                          .ifPresent(eventStoreTransformationRepository::delete);
        eventStoreTransformationProgressRepository.findById(transformationId)
                                                  .ifPresent(eventStoreTransformationProgressRepository::delete);
        activeTransformations.remove(transformationId);
        transformationStoreRegistry.delete(transformationId);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void setTransformationStatus(String transformationId, EventStoreTransformationJpa.Status status) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                          .orElseThrow(() -> new RuntimeException(
                                                                                                  "Transformation not found"));
        transformationJpa.setStatus(status);
        eventStoreTransformationRepository.save(transformationJpa);
        activeTransformations.computeIfPresent(transformationId,
                                               (id, active) -> active.withState(transformationJpa.getStatus()));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void startApply(String transformationId, boolean keepOldVersions, String appliedBy,
                           Date appliedDate, long firstEventToken, long lastEventToken) {
        EventStoreTransformationJpa transformationJpa = eventStoreTransformationRepository.findById(transformationId)
                                                                                          .orElseThrow(() -> new RuntimeException(
                                                                                                  "Transformation not found"));
        transformationJpa.setStatus(EventStoreTransformationJpa.Status.CLOSED);
        transformationJpa.setDateApplied(appliedDate);
        transformationJpa.setAppliedBy(appliedBy);
        transformationJpa.setKeepOldVersions(keepOldVersions);
        transformationJpa.setFirstEventToken(firstEventToken);
        transformationJpa.setLastEventToken(lastEventToken);
        eventStoreTransformationRepository.save(transformationJpa);
        activeTransformations.computeIfPresent(transformationId,
                                               (id, active) -> active.withState(transformationJpa.getStatus()));
        getOrCreateProgress(transformationId);
    }

    public Mono<Void> add(String transformationId, TransformEventRequest transformEventsRequest) {
        return transformationStoreRegistry.get(transformationId)
                                          .append(transformEventsRequest)
                                          .doOnSuccess(result -> activeTransformations.computeIfPresent(transformationId,
                                                                                                        (key, old) -> old.withLastToken(
                                                                                                                token(transformEventsRequest))));
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void setProgress(String transformationId, TransformationProgress transformationProgress) {
        EventStoreTransformationProgressJpa transformationJpa =
                eventStoreTransformationProgressRepository.findById(transformationId)
                                                          .orElseThrow(() -> new RuntimeException(
                                                                  "Transformation not found"));
        transformationJpa.setLastTokenApplied(transformationProgress.lastTokenProcessed());
        eventStoreTransformationProgressRepository.save(transformationJpa);
    }

    public EventStoreTransformationJpa.Status status(String transformationId) {
        EventStoreTransformationJpa transformationJpa =
                eventStoreTransformationRepository.findById(transformationId)
                                                  .orElseThrow(() -> new RuntimeException(
                                                          "Transformation not found"));
        return transformationJpa.getStatus();
    }

    @Transactional
    public void complete(String transformationId) {
        activeTransformations.remove(transformationId);
        setTransformationStatus(transformationId, EventStoreTransformationJpa.Status.DONE);
        transformationStoreRegistry.delete(transformationId);
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
            activeTransformations.put(transformationId,
                                      new ActiveEventStoreTransformation(transformationId, context, status, -1));
        }
    }

    public List<EventStoreTransformationJpa> findTransformations(String context) {
        return eventStoreTransformationRepository.findByContext(context);
    }

    public Optional<EventStoreTransformationJpa> transformation(String transformationId) {
        return eventStoreTransformationRepository.findById(transformationId);
    }

    public EventStoreTransformationProgressJpa getOrCreateProgress(String transformationId) {
        return eventStoreTransformationProgressRepository.findById(transformationId)
                                                         .orElseGet(() -> {
                                                             EventStoreTransformationProgressJpa progress = new EventStoreTransformationProgressJpa();
                                                             progress.setTransformationId(transformationId);
                                                             return eventStoreTransformationProgressRepository.save(
                                                                     progress);
                                                         });
    }

    public Optional<EventStoreTransformationProgressJpa> progress(String transformationId) {
        return eventStoreTransformationProgressRepository.findById(transformationId);
    }


    public void completeProgress(String transformationId) {
        EventStoreTransformationProgressJpa transformation = getOrCreateProgress(transformationId);
        transformation.setCompleted(true);
        eventStoreTransformationProgressRepository.save(transformation);
        applyingTransformations.remove(transformationId);
    }

    public int nextVersion(String context) {
        AtomicInteger lastVersion = new AtomicInteger(0);
        findTransformations(context).forEach(t -> {
            lastVersion.updateAndGet(old -> Math.max(old, t.getVersion()));
        });
        return lastVersion.get() + 1;
    }

    public Mono<Long> firstToken(String transformationId) {
        return transformationStoreRegistry.get(transformationId).firstEntry()
                                          .map(this::token);
    }

    public void setIteratorForActiveTransformation(String id, CloseableIterator<SerializedEventWithToken> iterator) {
        activeTransformations.computeIfPresent(id,
                                               (transformationId, activeTransformation)
                                                       -> activeTransformation.withIterator(iterator));
    }

    public TransformationEntryStore entryStore(String transformationId) {
        return transformationStoreRegistry.get(transformationId);
    }

    public boolean isActive(String transformationId) {
        return applyingTransformations.contains(transformationId);
    }

    public void setApplying(String transformationId) {
        applyingTransformations.add(transformationId);
    }
}
