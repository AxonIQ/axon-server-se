/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.grpc.PlatformService;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rest API to send instuction to a client to reconnect to Axon Server.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@RestController("InstructionRestController")
@RequestMapping("/v1/instructions")
public class InstructionRestController {
    private final PlatformService platformService;

    public InstructionRestController(PlatformService platformService) {
        this.platformService = platformService;
    }

    @PatchMapping
    public String requestReconnect(@RequestParam(value="client") String client) {
        if( platformService.requestReconnect(client)) {
            return client + ": requested reconnect";
        } else {
            return client +  ": not connected to this server";
        }
    }
}
