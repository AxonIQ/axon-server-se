/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * gRPC server setup for handling requests from client applications.
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


    public Gateway(MessagingPlatformConfiguration messagingPlatformConfiguration, List<AxonServerClientService> axonServerClientServices,
                   AxonServerAccessController axonServerAccessController, LicenseAccessController licenseAccessController) {
        this.routingConfiguration = messagingPlatformConfiguration;
        this.axonServerClientServices = axonServerClientServices;
        this.axonServerAccessController = axonServerAccessController;
        this.licenseAccessController = licenseAccessController;
        this.executorService = Executors.newFixedThreadPool(routingConfiguration.getExecutorThreadCount(),
                                                            new CustomizableThreadFactory("grpc-executor-"));
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop() {
        executorService.shutdown();
        if(started) {
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
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(routingConfiguration.getPort())
                                                             .permitKeepAliveWithoutCalls(true)
                                                             .permitKeepAliveTime(routingConfiguration.getMinKeepAliveTime(), TimeUnit.MILLISECONDS);


        if( routingConfiguration.getMaxMessageSize() > 0) {
            serverBuilder.maxInboundMessageSize(routingConfiguration.getMaxMessageSize());
        }

        String sslMessage = "no SSL";
        if( routingConfiguration.getSsl() != null && routingConfiguration.getSsl().isEnabled()) {
            if( routingConfiguration.getSsl().getCertChainFile() == null) {
                throw new RuntimeException("axoniq.axonserver.ssl.cert-chain-file");
            }
            if( routingConfiguration.getSsl().getPrivateKeyFile() == null) {
                throw new RuntimeException("axoniq.axonserver.ssl.private-key-file");
            }
            serverBuilder.useTransportSecurity(new File(routingConfiguration.getSsl().getCertChainFile()),
                    new File(routingConfiguration.getSsl().getPrivateKeyFile()));
            sslMessage = "SSL enabled";
        }

        axonServerClientServices.forEach(serverBuilder::addService);


        // Note that the last interceptor is executed first
        serverBuilder.intercept(new GrpcBufferingInterceptor(routingConfiguration.getGrpcBufferedMessages()));
        if( routingConfiguration.getAccesscontrol().isEnabled()) {
            serverBuilder.intercept(new AuthenticationInterceptor(axonServerAccessController));
        }
        serverBuilder.intercept(new LicenseInterceptor(licenseAccessController));
        serverBuilder.intercept(new ContextInterceptor());

        if( routingConfiguration.getKeepAliveTime() > 0) {
            serverBuilder.keepAliveTime(routingConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS)
                         .keepAliveTimeout(routingConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS);
        }
        serverBuilder.executor(executorService);

        server = serverBuilder.build();

        try {
            server.start();

            logger.info("Axon Server Gateway started on port: {} - {}", routingConfiguration.getPort(), sslMessage);

            started = true;
        } catch (IOException e) {
            logger.error("Starting Axon Server Gateway failed - {}", e.getMessage(), e);
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
