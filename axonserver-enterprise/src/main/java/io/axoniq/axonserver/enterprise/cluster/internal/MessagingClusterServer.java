package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.cluster.grpc.LeaderElectionService;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.AxonServerInternalService;
import io.axoniq.axonserver.grpc.ContextInterceptor;
import io.axoniq.axonserver.grpc.GrpcBufferingInterceptor;
import io.axoniq.axonserver.licensing.Feature;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
@Component("MessagingClusterServer")
public class MessagingClusterServer implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(MessagingClusterServer.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final LogReplicationService logReplicationService;
    private final LeaderElectionService leaderElectionService;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final List<AxonServerInternalService> internalServices;
    private final FeatureChecker limits;
    private boolean started;
    private Server server;

    public MessagingClusterServer(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                  LogReplicationService logReplicationService,
                                  LeaderElectionService leaderElectionService,
                                  List<AxonServerInternalService> internalServices,
                                  FeatureChecker limits,
                                  ApplicationEventPublisher applicationEventPublisher) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.logReplicationService = logReplicationService;
        this.leaderElectionService = leaderElectionService;
        this.internalServices = internalServices;
        this.limits = limits;
        this.applicationEventPublisher = applicationEventPublisher;
    }


    @Override
    public boolean isAutoStartup() {
        return Feature.CLUSTERING.enabled(limits);
    }

    @Override
    public void stop(Runnable callback) {
        try {
            server.shutdown().awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.debug("Interrupted during shutdown of internal AxonServer", e);
            Thread.currentThread().interrupt();
        }
        started = false;
        callback.run();
    }

    @Override
    public void start() {
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(messagingPlatformConfiguration.getInternalPort())
                                                             .permitKeepAliveTime(messagingPlatformConfiguration
                                                                                          .getMinKeepAliveTime(),
                                                                                  TimeUnit.MILLISECONDS)
                                                             .permitKeepAliveWithoutCalls(true);


        if (messagingPlatformConfiguration.getMaxMessageSize() > 0) {
            serverBuilder.maxInboundMessageSize(messagingPlatformConfiguration.getMaxMessageSize());
        }
        String sslMessage = "no SSL";
        if (messagingPlatformConfiguration.getSsl() != null && messagingPlatformConfiguration.getSsl().isEnabled()) {
            if (messagingPlatformConfiguration.getSsl().getInternalCertChainFile() == null) {
                throw new RuntimeException("axoniq.axonserver.ssl.cert-chain-file");
            }
            if (messagingPlatformConfiguration.getSsl().getPrivateKeyFile() == null) {
                throw new RuntimeException("axoniq.axonserver.ssl.private-key-file");
            }
            serverBuilder.useTransportSecurity(new File(messagingPlatformConfiguration.getSsl()
                                                                                      .getInternalCertChainFile()),
                                               new File(messagingPlatformConfiguration.getSsl().getPrivateKeyFile()));
            sslMessage = "SSL enabled";
        }

        serverBuilder.intercept(new InternalAuthenticationInterceptor(messagingPlatformConfiguration));

        internalServices.forEach(service -> {
            if (service.requiresContextInterceptor()) {
                serverBuilder.addService(ServerInterceptors.intercept(service,
                                                                      new ContextInterceptor()));
            } else {
                serverBuilder.addService(service);
            }
        });
        serverBuilder.addService(leaderElectionService);
        serverBuilder.addService(logReplicationService);

        if (messagingPlatformConfiguration.getKeepAliveTime() > 0) {
            serverBuilder.keepAliveTime(messagingPlatformConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS);
            serverBuilder.keepAliveTimeout(messagingPlatformConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS);
        }
        serverBuilder.intercept(new GrpcBufferingInterceptor(messagingPlatformConfiguration.getGrpcBufferedMessages()));

        serverBuilder.executor(Executors.newFixedThreadPool(messagingPlatformConfiguration
                                                                    .getClusterExecutorThreadCount(),
                                                            new CustomizableThreadFactory("cluster-executor-")));

        if (Epoll.isAvailable()) {
            serverBuilder.bossEventLoopGroup(new EpollEventLoopGroup(1,
                                                                     new CustomizableThreadFactory("cluster-epoll-boss-")));
            serverBuilder.workerEventLoopGroup(new EpollEventLoopGroup(0,
                                                                       new CustomizableThreadFactory(
                                                                               "cluster-epoll-worker-")));
            serverBuilder.channelType(EpollServerSocketChannel.class);
        } else {
            serverBuilder.bossEventLoopGroup(new NioEventLoopGroup(1,
                                                                   new CustomizableThreadFactory("cluster-nio-boss-")));
            serverBuilder.workerEventLoopGroup(new NioEventLoopGroup(0,
                                                                     new CustomizableThreadFactory("cluster-nio-worker-")));
            serverBuilder.channelType(NioServerSocketChannel.class);
        }

        server = serverBuilder.build();
        try {
            server.start();

            logger.info("Axon Server Cluster Server started on port: {} - {}",
                        messagingPlatformConfiguration.getInternalPort(),
                        sslMessage);
            applicationEventPublisher.publishEvent(new ReplicationServerStarted());
            started = true;
        } catch (IOException e) {
            logger.error("Starting Axon Server Cluster Server failed - {}", e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        stop(() -> {
        });
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    @Override
    public int getPhase() {
        return 50;
    }
}
