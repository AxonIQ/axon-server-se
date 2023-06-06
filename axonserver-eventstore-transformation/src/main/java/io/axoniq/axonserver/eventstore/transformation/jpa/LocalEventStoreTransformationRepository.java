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
 * JpaRepository for {@link LocalEventStoreTransformationJpa} entity.
 *
 * @author Sara Pellegirini
 * @author Milan Savic
 * @author Marc Gathier
 * @since 2023.0.0
 */
public interface LocalEventStoreTransformationRepository
        extends JpaRepository<LocalEventStoreTransformationJpa, String> {

    /**
     * Increments the last sequence of the specified transaction by the specified increment.
     *
     * @param transformationId the identifier of the transformation
     * @param increment        the delta used to increase the current last sequence
     */
    @Modifying
    @Query("update LocalEventStoreTransformationJpa d set d.lastSequenceApplied = d.lastSequenceApplied + :#{#increment} where d.transformationId = :#{#transformationId}")
    @Transactional
    void incrementLastSequence(@Param("transformationId") String transformationId, @Param("increment") long increment);


    @Modifying
    @Query("delete LocalEventStoreTransformationJpa d where d.transformationId not in "
            + "(select t.transformationId from EventStoreTransformationJpa t)")
    @Transactional
    void deleteOrphans();
}
