/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.impl.EventStoreTransformationJpa;
import io.axoniq.axonserver.eventstore.transformation.impl.EventStoreTransformationRepository;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.util.Collection;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@RestController
public class TransformationAdminRestController {

    private final EventStoreTransformationService eventStoreTransformationService;
    private final EventStoreTransformationRepository eventStoreTransformationRepository;

    public TransformationAdminRestController(
            EventStoreTransformationService eventStoreTransformationService,
            EventStoreTransformationRepository eventStoreTransformationRepository) {
        this.eventStoreTransformationService = eventStoreTransformationService;
        this.eventStoreTransformationRepository = eventStoreTransformationRepository;
    }

    @DeleteMapping("v1/transformations")
    public void cancelTransformation(@RequestParam("id") String id,
                                     @RequestParam(name = "targetContext", required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context) {
        eventStoreTransformationService.cancelTransformation(context, id).block();
    }

    @PostMapping("v1/transformations")
    public void applyTransformation(@RequestParam("id") String id,
                                    @RequestParam("lastToken") long lastToken,
                                    @RequestParam(name = "targetContext", required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context,
                                    @ApiIgnore final Authentication principal
    ) {
        eventStoreTransformationService.applyTransformation(context, id, lastToken, false,
                                                            principal == null ? "": principal.getName())
                .subscribe(v -> System.out.println("Done"), t -> t.printStackTrace());
    }

    @GetMapping("v1/transformations")
    public Collection<EventStoreTransformationJpa> get() {
        return eventStoreTransformationRepository.findAll();
    }
}
