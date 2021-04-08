/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.refactoring.transport.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.refactoring.version.VersionInfo;
import io.axoniq.axonserver.refactoring.version.VersionInfoProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
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
    private final Logger logger = LoggerFactory.getLogger(AxonServer.class);

    private final VersionInfoProvider versionInfoProvider;

    public AxonServer(VersionInfoProvider versionInfoProvider) {
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
        SpringApplication.run(AxonServer.class, args);
    }

    @PreDestroy
    public void clean() {
        GrpcFlowControlledDispatcherListener.shutdown();
    }

}
