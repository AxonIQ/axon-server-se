/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.LicenseAccessController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.FailedToStartException;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.AdvancedTlsX509KeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * gRPC server setup for handling requests from client applications.
 *
 * @author Marc Gathier
 */
@Component("Gateway")
public class Gateway implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(Gateway.class);
    private final List<AxonServerClientService> axonServerClientServices;
    private final AxonServerAccessController axonServerAccessController;
    private boolean started;
    private Server server;
    private final MessagingPlatformConfiguration routingConfiguration;
    private final LicenseAccessController licenseAccessController;
    private final ExecutorService executorService;
    private AdvancedTlsX509KeyManager.Closeable serverKeyClosable;
    private final Supplier<ScheduledExecutorService> maintenanceSchedulerSupplier;

    public Gateway(MessagingPlatformConfiguration messagingPlatformConfiguration,
                   List<AxonServerClientService> axonServerClientServices,
                   AxonServerAccessController axonServerAccessController,
                   LicenseAccessController licenseAccessController) {
        this(messagingPlatformConfiguration,
             axonServerClientServices,
             axonServerAccessController,
             licenseAccessController,
             Executors::newSingleThreadScheduledExecutor);
    }

    @Autowired
    public Gateway(MessagingPlatformConfiguration messagingPlatformConfiguration,
                   List<AxonServerClientService> axonServerClientServices,
                   AxonServerAccessController axonServerAccessController,
                   LicenseAccessController licenseAccessController,
                   Supplier<ScheduledExecutorService> maintenanceSchedulerSupplier) {
        this.routingConfiguration = messagingPlatformConfiguration;
        this.axonServerClientServices = axonServerClientServices;
        this.axonServerAccessController = axonServerAccessController;
        this.licenseAccessController = licenseAccessController;
        this.maintenanceSchedulerSupplier = maintenanceSchedulerSupplier;
        this.executorService = Executors.newFixedThreadPool(routingConfiguration.getExecutorThreadCount(),
                                                            new CustomizableThreadFactory("grpc-executor-"));
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop() {
        if (serverKeyClosable != null) {
            serverKeyClosable.close();
        }
        executorService.shutdown();
        if (started) {
            try {
                if (!server.shutdown().awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.debug("Forcefully stopping Cluster Server");
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.debug("Interrupted during shutdown of GRPC server", e);
                Thread.currentThread().interrupt();
            }
        }

        started = false;
        logger.info("Axon Server Gateway stopped");
    }

    @Override
    public void start() {
        NettyServerBuilder serverBuilder;
        String sslMessage = "no SSL";

        try {
            if (routingConfiguration.getSsl() != null && routingConfiguration.getSsl().isEnabled()) {
                AdvancedTlsX509KeyManager serverKeyManager = new AdvancedTlsX509KeyManager();
                serverKeyClosable = serverKeyManager.updateIdentityCredentialsFromFile(
                        new File(routingConfiguration.getSsl().getPrivateKeyFile()),
                        new File(routingConfiguration.getSsl().getCertChainFile()),
                        1,
                        TimeUnit.MINUTES,
                        maintenanceSchedulerSupplier.get());
                ServerCredentials serverCredentials = TlsServerCredentials.newBuilder()
                                                                          .keyManager(serverKeyManager)
                                                                          .clientAuth(TlsServerCredentials.ClientAuth.NONE)
                                                                          .build();
                sslMessage = "SSL enabled";
                serverBuilder = NettyServerBuilder.forPort(routingConfiguration.getPort(),
                                                           serverCredentials);
            } else {
                serverBuilder = NettyServerBuilder.forPort(routingConfiguration.getPort());
            }
            if (routingConfiguration.getMaxMessageSize() > 0) {
                serverBuilder.maxInboundMessageSize(routingConfiguration.getMaxMessageSize());
            }
            serverBuilder
                    .permitKeepAliveWithoutCalls(true)
                    .permitKeepAliveTime(routingConfiguration.getMinKeepAliveTime(), TimeUnit.MILLISECONDS);


            axonServerClientServices.forEach(serverBuilder::addService);

            // Note that the last interceptor is executed first
            serverBuilder.intercept(new GrpcBufferingInterceptor(routingConfiguration.getGrpcBufferedMessages()));
            if (routingConfiguration.getAccesscontrol().isEnabled()) {
                serverBuilder.intercept(new AuthenticationInterceptor(axonServerAccessController));
            }
            serverBuilder.intercept(new LicenseInterceptor(licenseAccessController));
            serverBuilder.intercept(new ContextInterceptor());

            if (routingConfiguration.getKeepAliveTime() > 0) {
                serverBuilder.keepAliveTime(routingConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS)
                             .keepAliveTimeout(routingConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS);
            }
            serverBuilder.executor(executorService);

            server = serverBuilder.build();

            server.start();

            logger.info("Axon Server Gateway started on port: {} - {}", routingConfiguration.getPort(), sslMessage);

            started = true;
        } catch (Exception e) {
            throw new FailedToStartException("Starting Axon Server Gateway failed", e);
        }
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    @Override
    public int getPhase() {
        return 200;
    }
}
