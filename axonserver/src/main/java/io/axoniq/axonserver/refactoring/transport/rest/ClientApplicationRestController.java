/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.rest;

import io.axoniq.axonserver.refactoring.client.ComponentItems;
import io.axoniq.axonserver.refactoring.client.instance.Client;
import io.axoniq.axonserver.refactoring.client.instance.Clients;
import io.axoniq.axonserver.refactoring.security.AuditLog;
import io.axoniq.axonserver.refactoring.transport.ClientIdRegistry;
import io.axoniq.axonserver.refactoring.transport.rest.serializer.Printable;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * REST controller to retrieve details about client applications
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@RestController
@RequestMapping("v1/components")
public class ClientApplicationRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final Clients clients;
    private final ClientIdRegistry clientIdRegistry;

    public ClientApplicationRestController(Clients clients, ClientIdRegistry clientIdRegistry) {
        this.clients = clients;
        this.clientIdRegistry = clientIdRegistry;
    }

    /**
     * retrieve instances of a specific client application (component)
     *
     * @param component the name of the component to retrieve
     * @param context   the required context
     * @return
     */
    @GetMapping("{component}/instances")
    public Iterable getComponentInstances(@PathVariable("component") String component,
                                          @RequestParam("context") String context,
                                          @ApiIgnore Principal principal) {
        auditLog.info("[{}] Request for a list of clients belonging to component \"{}\" and context=\"{}\"",
                      AuditLog.username(principal),
                      component,
                      context);

        return new ComponentItems<>(component, context, clients);
    }

    /**
     * returns a list of currently connected clients
     *
     * @return stream containing all currently connected clients
     */
    @GetMapping("clients")
    public Stream<Client> listClients(@ApiIgnore Principal principal) {
        auditLog.info("[{}] Request for a list of all connected clients.", AuditLog.username(principal));

        return StreamSupport.stream(clients.spliterator(), false);
    }

    @GetMapping("clientIds")
    public Printable listClientIds(@ApiIgnore Principal principal) {
        auditLog.info("[{}] Request for a list of all connected clients.", AuditLog.username(principal));

        return clientIdRegistry;
    }
}
