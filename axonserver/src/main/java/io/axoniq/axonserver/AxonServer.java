/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PreDestroy;

/**
 * Main class for AxonServer.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@SpringBootApplication
@EnableAsync
@EnableScheduling
public class AxonServer {
    public static void main(String[] args) {
        System.setProperty("spring.config.name", "axonserver");
        SpringApplication.run(AxonServer.class, args);
    }

    @PreDestroy
    public void clean() {
        GrpcFlowControlledDispatcherListener.shutdown();
    }

}
