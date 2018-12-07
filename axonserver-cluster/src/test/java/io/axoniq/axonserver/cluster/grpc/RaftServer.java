package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftServerConfiguration;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RaftServer {
    private final static Logger logger = LoggerFactory.getLogger(RaftServer.class);

    private Server server;
    private boolean started;
    private final RaftServerConfiguration messagingPlatformConfiguration;
    private final LeaderElectionService leaderElectionService;
    private final LogReplicationService logReplicationService;

    public RaftServer(RaftServerConfiguration messagingPlatformConfiguration, LeaderElectionService leaderElectionService, LogReplicationService logReplicationService) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.leaderElectionService = leaderElectionService;
        this.logReplicationService = logReplicationService;
    }

    public void stop(Runnable callback) {
        try {
            server.shutdown().awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.debug("Interrupted during shutdown of gRPC Messaging Cluster Server", e);
            Thread.currentThread().interrupt();
        }
        started = false;
        callback.run();
    }

    public void start() {
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(messagingPlatformConfiguration.getInternalPort())
                .permitKeepAliveTime(messagingPlatformConfiguration.getMinKeepAliveTime(), TimeUnit.MILLISECONDS)
                .permitKeepAliveWithoutCalls(true);


        if( messagingPlatformConfiguration.getMaxMessageSize() > 0) {
            serverBuilder.maxInboundMessageSize(messagingPlatformConfiguration.getMaxMessageSize());
        }
        String sslMessage = "no SSL";
        serverBuilder.addService(logReplicationService);
        serverBuilder.addService(leaderElectionService);

        if( messagingPlatformConfiguration.getKeepAliveTime() > 0) {
            serverBuilder.keepAliveTime(messagingPlatformConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS);
            serverBuilder.keepAliveTimeout(messagingPlatformConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS);
        }

        serverBuilder.directExecutor();

        server = serverBuilder.build();
        try {
            server.start();

            logger.info("gRPC Messaging Cluster Server started on port: {} - {}", messagingPlatformConfiguration.getInternalPort(), sslMessage);

            started = true;
        } catch (IOException e) {
            logger.error("Starting gRPC Messaging Cluster Server gateway failed - {}", e.getMessage(), e);
        }
    }

}
