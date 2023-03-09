/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.rest.EventsRestController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;
import java.util.UUID;
import javax.validation.Valid;

import static io.axoniq.axonserver.AxonServerAccessController.CONTEXT_PARAM;
import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;

/**
 * Rest controller for event transformation.
 *
 * @author Sara Pellegrini
 * @author Marc Gathier
 * @since 4.6.0
 */
@RestController
@ConditionalOnProperty(value = "axoniq.axonserver.preview.event-transformation")
public class EventStoreTransformationRestController {

    private final EventStoreTransformationService eventStoreTransformationService;

    /**
     * Construct a rest controller that delegates the requests to the specified service.
     *
     * @param eventStoreTransformationService the service to be invoked to handle the requests
     */
    public EventStoreTransformationRestController(EventStoreTransformationService eventStoreTransformationService) {
        this.eventStoreTransformationService = eventStoreTransformationService;
    }

    /**
     * Initializes a new transformation and returns a transformation id.
     *
     * @param context     the name of the context
     * @param description description of the goal of the transformation
     * @param principal   authentication of the user/application requesting the service
     * @return a mono with a unique identifier for the transformation
     */
    @PostMapping("v1/transformations")
    public String startTransformation(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = DEFAULT_CONTEXT, required = false) String context,
            @RequestParam("description") String description,
            @ApiIgnore final Principal principal
    ) {
        String uuid = UUID.randomUUID().toString();
        eventStoreTransformationService.start(uuid, context, description, new PrincipalAuthentication(principal))
                                       .block();
        return uuid;
    }

    /**
     * Registers the intent to delete an event when applying the transformation. The caller needs to provide the
     * previous sequence to ensure that the transformation actions are received in the correct order.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to be deleted
     * @param sequence         the sequence of the transformation request used to validate the request chain, -1 if it
     *                         is the first one
     * @param principal        authentication of the user/application requesting the service
     */
    @PatchMapping("v1/transformations/{transformationId}/event/delete/{token}")
    public void deleteEvent(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = DEFAULT_CONTEXT, required = false) String context,
            @PathVariable("transformationId") String transformationId,
            @PathVariable("token") long token,
            @RequestParam("sequence") long sequence,
            @ApiIgnore final Principal principal) {
        eventStoreTransformationService.deleteEvent(context,
                                                    transformationId,
                                                    token,
                                                    sequence,
                                                    new PrincipalAuthentication(principal))
                                       .block();
    }

    /**
     * Registers the intent to replace the content of an event when applying the transformation. The caller needs to
     * provide the previous sequence to ensure that the transformation actions are received in the correct order.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param token            the token (global position) of the event to be replaced
     * @param event            the new content of the event
     * @param sequence         the sequence of the transformation request used to validate the request chain, -1 if it
     *                         is the first one
     * @param principal        authentication of the user/application requesting the service
     */
    @PatchMapping("v1/transformations/{transformationId}/event/replace/{token}")
    public void replaceEvent(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = DEFAULT_CONTEXT, required = false) String context,
            @PathVariable("transformationId") String transformationId,
            @PathVariable("token") long token,
            @Valid @RequestBody EventsRestController.JsonEvent event,
            @RequestParam("sequence") long sequence,
            @ApiIgnore final Principal principal
    ) {
        eventStoreTransformationService.replaceEvent(context, transformationId, token, event.asEvent(), sequence,
                                                     new PrincipalAuthentication(principal))
                                       .block();
    }

    /**
     * Cancels a transformation. Can only be done before calling the startApplying operation for the same
     * transformation.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param principal        authentication of the user/application requesting the service
     */
    @PatchMapping("v1/transformations/{transformationId}/cancel")
    public void cancelTransformation(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = DEFAULT_CONTEXT, required = false) String context,
            @PathVariable("transformationId") String transformationId,
            @ApiIgnore final Principal principal) {
        eventStoreTransformationService.cancel(context, transformationId, new PrincipalAuthentication(principal))
                                       .block();
    }

    /**
     * Starts the apply process. The process runs in the background.
     *
     * @param context          the name of the context
     * @param transformationId the identification of the transformation
     * @param lastSequence     the sequence of the transformation request used to validate the request chain, -1 if it
     *                         is the first one
     * @param principal        authentication of the user/application requesting the service
     */
    @PatchMapping("v1/transformations/{transformationId}/apply")
    public void applyTransformation(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = DEFAULT_CONTEXT, required = false) String context,
            @PathVariable("transformationId") String transformationId,
            @RequestParam("lastSequence") long lastSequence,
            @ApiIgnore final Authentication principal
    ) {
        eventStoreTransformationService.startApplying(context,
                                                      transformationId,
                                                      lastSequence,
                                                      new PrincipalAuthentication(principal))
                                       .block();
    }

    /**
     * Deletes old versions of segments updated by a transformation
     *
     * @param context   the name of the context
     * @param principal authentication of the user/application requesting the service
     */
    @PostMapping("v1/eventstore/compact")
    public void compact(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = DEFAULT_CONTEXT, required = false) String context,
            @ApiIgnore final Authentication principal
    ) {
        String uuid = UUID.randomUUID().toString();
        eventStoreTransformationService.startCompacting(uuid, context, new PrincipalAuthentication(principal))
                                       .block();
    }

    /**
     * Returns the transformations for the specified context.
     *
     * @param context   the name of the context
     * @param principal authentication of the user/application requesting the service
     * @return the transformations for the specified context.
     */
    @GetMapping("v1/transformations")
    public Flux<ObjectNode> get(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = DEFAULT_CONTEXT, required = false) String context,
            @ApiIgnore final Authentication principal) {
        return eventStoreTransformationService.transformations(context, new PrincipalAuthentication(principal))
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
