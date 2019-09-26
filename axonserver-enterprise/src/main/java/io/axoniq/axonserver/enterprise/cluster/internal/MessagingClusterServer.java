package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.cluster.grpc.LeaderElectionService;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftConfigService;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftGroupService;
import io.axoniq.axonserver.grpc.GrpcBufferingInterceptor;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.grpc.ContextInterceptor;
import io.axoniq.axonserver.saas.SaasAdminService;
import io.axoniq.axonserver.saas.SaasUserService;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
@Component("MessagingClusterServer")
public class MessagingClusterServer implements SmartLifecycle{
    private final Logger logger = LoggerFactory.getLogger(MessagingClusterServer.class);
    private boolean started;

    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final MessagingClusterService messagingClusterService;
    private final InternalEventStoreService internalEventStoreService;
    private final LogReplicationService logReplicationService;
    private final LeaderElectionService leaderElectionService;
    private final GrpcRaftGroupService grpcRaftGroupService;
    private final GrpcRaftConfigService grpcRaftConfigService;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final SaasAdminService saasAdminService;
    private final SaasUserService saasUserService;
    private final FeatureChecker limits;
    private Server server;

    public MessagingClusterServer(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                  MessagingClusterService messagingClusterService,
                                  InternalEventStoreService internalEventStoreService,
                                  LogReplicationService logReplicationService,
                                  LeaderElectionService leaderElectionService,
                                  GrpcRaftGroupService grpcRaftGroupService,
                                  GrpcRaftConfigService grpcRaftConfigService,
                                  SaasAdminService saasAdminService,
                                  SaasUserService saasUserService,
                                  FeatureChecker limits,
                                  ApplicationEventPublisher applicationEventPublisher) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.messagingClusterService = messagingClusterService;
        this.internalEventStoreService = internalEventStoreService;
        this.logReplicationService = logReplicationService;
        this.leaderElectionService = leaderElectionService;
        this.grpcRaftGroupService = grpcRaftGroupService;
        this.grpcRaftConfigService = grpcRaftConfigService;
        this.saasAdminService = saasAdminService;
        this.saasUserService = saasUserService;
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
                .permitKeepAliveTime(messagingPlatformConfiguration.getMinKeepAliveTime(), TimeUnit.MILLISECONDS)
                .permitKeepAliveWithoutCalls(true);


        if( messagingPlatformConfiguration.getMaxMessageSize() > 0) {
            serverBuilder.maxInboundMessageSize(messagingPlatformConfiguration.getMaxMessageSize());
        }
        String sslMessage = "no SSL";
        if( messagingPlatformConfiguration.getSsl() != null && messagingPlatformConfiguration.getSsl().isEnabled()) {
            if( messagingPlatformConfiguration.getSsl().getInternalCertChainFile() == null) {
                throw new RuntimeException("axoniq.axonserver.ssl.cert-chain-file");
            }
            if( messagingPlatformConfiguration.getSsl().getPrivateKeyFile() == null) {
                throw new RuntimeException("axoniq.axonserver.ssl.private-key-file");
            }
            serverBuilder.useTransportSecurity(new File(messagingPlatformConfiguration.getSsl().getInternalCertChainFile()),
                    new File(messagingPlatformConfiguration.getSsl().getPrivateKeyFile()));
            sslMessage = "SSL enabled";
        }
        serverBuilder.addService(messagingClusterService);
        serverBuilder.addService(leaderElectionService);
        serverBuilder.addService(logReplicationService);
        serverBuilder.addService(internalEventStoreService);
        serverBuilder.addService(grpcRaftGroupService);
        serverBuilder.addService(grpcRaftConfigService);
        serverBuilder.addService(saasAdminService);

        if( messagingPlatformConfiguration.getAccesscontrol() != null && messagingPlatformConfiguration.getAccesscontrol().isEnabled()) {
            serverBuilder.addService(ServerInterceptors.intercept(messagingClusterService, new InternalAuthenticationInterceptor(messagingPlatformConfiguration)));
            serverBuilder.addService(ServerInterceptors.intercept(leaderElectionService, new InternalAuthenticationInterceptor(messagingPlatformConfiguration)));
            serverBuilder.addService(ServerInterceptors.intercept(logReplicationService, new InternalAuthenticationInterceptor(messagingPlatformConfiguration)));
            serverBuilder.addService(ServerInterceptors.intercept(internalEventStoreService, new InternalAuthenticationInterceptor(messagingPlatformConfiguration),
                                                                  new ContextInterceptor()));
            serverBuilder.addService(ServerInterceptors.intercept(grpcRaftGroupService, new InternalAuthenticationInterceptor(messagingPlatformConfiguration)));
            serverBuilder.addService(ServerInterceptors.intercept(grpcRaftConfigService, new InternalAuthenticationInterceptor(messagingPlatformConfiguration)));
            serverBuilder.addService(ServerInterceptors.intercept(saasAdminService,
                                                                  new InternalAuthenticationInterceptor(
                                                                          messagingPlatformConfiguration)));
            serverBuilder.addService(ServerInterceptors.intercept(saasUserService,
                                                                  new InternalAuthenticationInterceptor(
                                                                          messagingPlatformConfiguration),
                                                                  new ContextInterceptor()));
        } else {
            serverBuilder.addService(ServerInterceptors.intercept(internalEventStoreService, new ContextInterceptor()));
            serverBuilder.addService(ServerInterceptors.intercept(saasUserService, new ContextInterceptor()));
        }
        if( messagingPlatformConfiguration.getKeepAliveTime() > 0) {
            serverBuilder.keepAliveTime(messagingPlatformConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS);
            serverBuilder.keepAliveTimeout(messagingPlatformConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS);
        }
        serverBuilder.intercept(new GrpcBufferingInterceptor(messagingPlatformConfiguration.getGrpcBufferedMessages()));

        serverBuilder.executor(Executors.newCachedThreadPool());


        server = serverBuilder.build();
        try {
            server.start();

            logger.info("Axon Server Cluster Server started on port: {} - {}", messagingPlatformConfiguration.getInternalPort(), sslMessage);
            applicationEventPublisher.publishEvent(new ReplicationServerStarted());
            started = true;
        } catch (IOException e) {
            logger.error("Starting Axon Server Cluster Server failed - {}", e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        stop(() ->{});
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
