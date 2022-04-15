/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface EventStoreTransformationRepository extends JpaRepository<EventStoreTransformationJpa, String> {

    /**
     * Find all transformations for a context.
     *
     * @param context the name of the context
     * @return a list of transformations for the context
     */
    List<EventStoreTransformationJpa> findByContext(String context);

    List<EventStoreTransformationJpa> findAllByStatus(EventStoreTransformationJpa.Status status);

    Optional<EventStoreTransformationJpa> findEventStoreTransformationJpaByContextAndStatus(String context, EventStoreTransformationJpa.Status status);

    default Optional<EventStoreTransformationJpa> findActiveByContext(String context) {
        return findEventStoreTransformationJpaByContextAndStatus(context, EventStoreTransformationJpa.Status.ACTIVE);
    }

    @Query("select max(version) from EventStoreTransformationJpa where context = ?1 and status = EventStoreTransformationJpa.Status.ACTIVE")
    Optional<Integer> lastAppliedVersion(String context);
}
