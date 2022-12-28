/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
@RestController
public class TransformationAdminRestController {

    private final EventStoreTransformationService eventStoreTransformationService;

    public TransformationAdminRestController(EventStoreTransformationService eventStoreTransformationService) {
        this.eventStoreTransformationService = eventStoreTransformationService;
    }

    @DeleteMapping("v1/transformations")
    public void cancelTransformation(@RequestParam("id") String id,
                                     @RequestParam(name = "targetContext", required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context,
                                     @ApiIgnore final Principal principal) {
        eventStoreTransformationService.cancel(context, id, new PrincipalAuthentication(principal))
                                       .block();
    }

    @PostMapping("v1/transformations")
    public void applyTransformation(@RequestParam("id") String id,
                                    @RequestParam("lastToken") long lastToken,
                                    @RequestParam(name = "targetContext", required = false, defaultValue = Topology.DEFAULT_CONTEXT) String context,
                                    @ApiIgnore final Authentication principal
    ) {
        eventStoreTransformationService.startApplying(context, id, lastToken, new PrincipalAuthentication(principal))
                                       .block();
    }

    @GetMapping("v1/transformations")
    public Flux<ObjectNode> get(@ApiIgnore final Authentication principal) {
        return eventStoreTransformationService.transformations(new PrincipalAuthentication(principal))
                                              .map(this::toJson);
    }

    private ObjectNode toJson(EventStoreTransformationService.Transformation t) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("id", t.id());
        node.put("lastSequence", t.lastSequence().orElse(null));
        node.put("status", t.status().name());
        return node;
    }
}
