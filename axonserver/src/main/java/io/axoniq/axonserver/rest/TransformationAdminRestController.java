/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.requestprocessor.eventstore.DefaultEventStoreTransformationService;
import io.axoniq.axonserver.requestprocessor.eventstore.EventStoreTransformationJpa;
import io.axoniq.axonserver.requestprocessor.eventstore.EventStoreTransformationRepository;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@RestController
public class TransformationAdminRestController {

    private final DefaultEventStoreTransformationService eventStoreTransformationService;
    private final EventStoreTransformationRepository eventStoreTransformationRepository;

    public TransformationAdminRestController(
            DefaultEventStoreTransformationService eventStoreTransformationService,
            EventStoreTransformationRepository eventStoreTransformationRepository) {
        this.eventStoreTransformationService = eventStoreTransformationService;
        this.eventStoreTransformationRepository = eventStoreTransformationRepository;
    }

    @DeleteMapping("v1/transformations")
    public void cancelTransformation() {
        eventStoreTransformationService.cancelTransformation(Topology.DEFAULT_CONTEXT);
    }

    @PostMapping("v1/transformations")
    public void applyTransformation() {
        eventStoreTransformationService.applyTransformation(Topology.DEFAULT_CONTEXT);
    }

    @GetMapping("v1/transformations")
    public Collection<EventStoreTransformationJpa> get() {
        return eventStoreTransformationRepository.findAll();
    }
}
