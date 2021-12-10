/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

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

    default List<EventStoreTransformationJpa> findAllClosed() {
        return findAllByStatus(EventStoreTransformationJpa.Status.CLOSED);
    }
}
