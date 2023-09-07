/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.version.VersionInfo;
import io.axoniq.axonserver.version.VersionInfoProvider;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main class for AxonServer.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@SpringBootApplication
@EnableAsync
@EnableScheduling
@OpenAPIDefinition
public class StandardAxonServer {
    private final Logger logger = LoggerFactory.getLogger(StandardAxonServer.class);

    private final VersionInfoProvider versionInfoProvider;

    public StandardAxonServer(VersionInfoProvider versionInfoProvider) {
        this.versionInfoProvider = versionInfoProvider;
    }

    @PostConstruct
    public void versionSet() {
        VersionInfo versionInfo = versionInfoProvider.get();
        if (versionInfo != null) {
            logger.info("{} version {}", versionInfo.getProductName(), versionInfo.getVersion());
        }
    }



    public static void main(String[] args) {
        System.setProperty("spring.config.name", "axonserver");
        SpringApplication.run(StandardAxonServer.class, args);
    }

    @PreDestroy
    public void clean() {
        GrpcFlowControlledDispatcherListener.shutdown();
    }

}
