/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * REST controller to retrieve instances of a specific client application (component)
 * @author Sara Pellegrini
 * @since 4.0
 */
@RestController
@RequestMapping("v1/components")
public class ClientApplicationRestController {

    private final Clients clients;

    public ClientApplicationRestController(Clients clients) {
        this.clients = clients;
    }

    @GetMapping("{component}/instances")
    public Iterable getComponentInstances(@PathVariable("component") String component, @RequestParam("context") String context ){
        return new ComponentItems<>(component,context,clients);
    }

    @GetMapping("clients")
    public Stream<Client> listClients() {
        return StreamSupport.stream(clients.spliterator(),false);
    }

}
