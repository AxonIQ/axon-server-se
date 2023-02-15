/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import javax.transaction.Transactional;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface LocalEventStoreTransformationRepository
        extends JpaRepository<LocalEventStoreTransformationJpa, String> {

    @Modifying
    @Query("update LocalEventStoreTransformationJpa d set d.lastSequenceApplied = d.lastSequenceApplied + :#{#increment} where d.transformationId = :#{#transformationId}")
    @Transactional
    void incrementLastSequence(@Param("transformationId") String transformationId, @Param("increment") long increment);
}
