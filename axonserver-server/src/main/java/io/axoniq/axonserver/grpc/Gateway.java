package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
@Component("Gateway")
public class Gateway implements SmartLifecycle {
    private final Logger logger = LoggerFactory.getLogger(Gateway.class);
    private final PlatformService instructionService;
    private final AxonServerAccessController axonHubAccessController;
    private boolean started;
    private Server server;
    private final MessagingPlatformConfiguration routingConfiguration;
    private final EventDispatcher eventDispatcher;
    private final CommandService commandService;
    private final QueryService queryService;


    public Gateway(MessagingPlatformConfiguration routingConfiguration, EventDispatcher eventDispatcher, CommandService commandService,
                   QueryService queryService, PlatformService instructionService,
                   AxonServerAccessController axonHubAccessController) {
        this.routingConfiguration = routingConfiguration;
        this.eventDispatcher = eventDispatcher;
        this.commandService = commandService;
        this.queryService = queryService;
        this.instructionService = instructionService;
        this.axonHubAccessController = axonHubAccessController;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        if(started) {
            try {
                server.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted during shutdown of GRPC server", e);
                Thread.currentThread().interrupt();
            }
        }

        started = false;
        callback.run();
    }

    @Override
    public void start() {
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(routingConfiguration.getPort())
                                                             .permitKeepAliveWithoutCalls(true)
                                                             .permitKeepAliveTime(routingConfiguration.getMinKeepAliveTime(), TimeUnit.MILLISECONDS);

//        if(Epoll.isAvailable()) {
//            serverBuilder.channelType(EpollServerSocketChannel.class);
//            if (routingConfiguration.getWorkerThreads() > 0) {
//                serverBuilder.workerEventLoopGroup(new EpollEventLoopGroup(routingConfiguration.getWorkerThreads()));
//            }
//            if (routingConfiguration.getBossThreads() > 0) {
//                serverBuilder.bossEventLoopGroup(new EpollEventLoopGroup(routingConfiguration.getBossThreads()));
//            }
//        } else {
//            serverBuilder.channelType(NioServerSocketChannel.class);
//            if (routingConfiguration.getWorkerThreads() > 0) {
//                serverBuilder.workerEventLoopGroup(new NioEventLoopGroup(routingConfiguration.getWorkerThreads()));
//            }
//            if (routingConfiguration.getBossThreads() > 0) {
//                serverBuilder.bossEventLoopGroup(new NioEventLoopGroup(routingConfiguration.getBossThreads()));
//            }
//        }

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

        serverBuilder
                .addService(commandService)
                .addService(queryService)
                .addService(eventDispatcher)
                .addService(instructionService);

        List<ServerInterceptor> interceptorList = new ArrayList<>();
        if( routingConfiguration.getAccesscontrol().isEnabled()) {
            interceptorList.add( new AuthenticationInterceptor(axonHubAccessController));
        }
        interceptorList.add(new ContextInterceptor());

        // Note that the last interceptor is executed first
        serverBuilder.addService(ServerInterceptors.intercept(commandService, interceptorList));
        serverBuilder.addService(ServerInterceptors.intercept(queryService, interceptorList));
        serverBuilder.addService(ServerInterceptors.intercept(eventDispatcher, interceptorList));
        serverBuilder.addService(ServerInterceptors.intercept(instructionService, interceptorList));

//        if( routingConfiguration.getKeepAliveTime() > 0) {
//            serverBuilder.keepAliveTime(routingConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS)
//                         .keepAliveTimeout(routingConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS);
//        }
//
//        if( routingConfiguration.getExecutorThreads() > 0) {
//            serverBuilder.executor(Executors.newFixedThreadPool(routingConfiguration.getExecutorThreads()));
//        }
        serverBuilder.directExecutor();

        server = serverBuilder.build();

        try {
            server.start();

            logger.info("gRPC Gateway started on port: {} - {}", routingConfiguration.getPort(), sslMessage);

            started = true;
        } catch (IOException e) {
            logger.error("Starting GRPC gateway failed - {}", e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        stop(()-> {});
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    @Override
    public int getPhase() {
        return 10;
    }
}
