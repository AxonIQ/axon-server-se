/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Marc Gathier
 */
@Component
public class LifecycleController {
    private static final String EVENT_STORE_SERVER_PID = "AxonIQ.pid";
    private final Logger logger = LoggerFactory.getLogger(LifecycleController.class);
    private boolean cleanShutdown=false;
    private final Path path;
    private final ApplicationContext applicationContext;

    public LifecycleController(ApplicationContext applicationContext, MessagingPlatformConfiguration configuration) {

        this.path = Paths.get(configuration.getPidFileLocation(), EVENT_STORE_SERVER_PID);
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void checkAndCreatePidFile() throws IOException {
        if (! path.toFile().exists()) {
            cleanShutdown = true;
        } else {
            Files.delete(path);
        }

        String jvmName = ManagementFactory.getRuntimeMXBean().getName();

        try(OutputStream is = Files.newOutputStream(path, StandardOpenOption.CREATE) ) {
            is.write(jvmName.getBytes(Charset.forName("UTF-8")));
        }
    }

    @PreDestroy
    public void removePidFile() {
        try {
            if( cleanShutdown) Files.delete(path);
        } catch (IOException e) {
            logger.info("Failed to remove {}", path);
        }
    }

    public boolean isCleanShutdown() {
        return cleanShutdown;
    }

    public void setCleanShutdown() {
        this.cleanShutdown = true;
    }

    Path getPath() {
        return path;
    }

    public void abort() {
        SpringApplication.exit(applicationContext, () -> {
            System.exit(99);
            return 99;
        });
    }

    public void licenseError(String parameter) {
        logger.error(parameter);
        SpringApplication.exit(applicationContext, () -> {
            System.exit(1);
            return 1;
        });
    }

}
