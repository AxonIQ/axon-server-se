package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

/**
 * @author Marc Gathier
 */
public class ManagedChannelHelper {
    private static final Logger logger = LoggerFactory.getLogger(ManagedChannelHelper.class);

    private static final Map<String, ManagedChannel> channelPerNode = new ConcurrentHashMap<>();

    public static ManagedChannel createManagedChannel(MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode) {

        return createManagedChannel(messagingPlatformConfiguration, clusterNode.getInternalHostName(), clusterNode.getGrpcInternalPort());
    }

    public static ManagedChannel createManagedChannel(MessagingPlatformConfiguration messagingPlatformConfiguration,  String host, int port) {

        String key = channelKey(host, port);
        if( channelPerNode.containsKey(key)) return channelPerNode.get(key);

        synchronized (channelPerNode) {
            if( channelPerNode.containsKey(key)) return channelPerNode.get(key);
            ManagedChannel channel = null;
            try {
                NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);

                if (messagingPlatformConfiguration.getKeepAliveTime() > 0) {
                    builder.keepAliveTime(messagingPlatformConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS)
                           .keepAliveTimeout(messagingPlatformConfiguration.getKeepAliveTimeout(),
                                             TimeUnit.MILLISECONDS)
                           .keepAliveWithoutCalls(true);
                }
                if (messagingPlatformConfiguration.getSsl() != null && messagingPlatformConfiguration.getSsl()
                                                                                                     .isEnabled()) {
                    addTlsConfig(messagingPlatformConfiguration, builder);
                } else {
                    builder.usePlaintext();
                }
                if (messagingPlatformConfiguration.getMaxMessageSize() > 0) {
                    builder.maxInboundMessageSize(messagingPlatformConfiguration.getMaxMessageSize());
                }
                builder.directExecutor();
                channel = builder.build();
                channelPerNode.put(key, channel);
            } catch (Exception ex) {
                logger.warn("Error while building channel - {}", ex.getMessage());
                return null;
            }
            return channel;
        }
    }

    @NotNull
    private static String channelKey(String host, int port) {
        return host + ":" + port;
    }

    private static void addTlsConfig(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                     NettyChannelBuilder builder) {
        if( messagingPlatformConfiguration.getSsl().getInternalTrustManagerFile() != null) {
            File trustManager = new File(messagingPlatformConfiguration.getSsl().getInternalTrustManagerFile());
            if (!trustManager.exists()) {
                throw new RuntimeException(
                        "TrustManager file " + trustManager.getAbsolutePath() + " does not exist");
            }
            SslContext sslContext = null;
            try {
                sslContext = GrpcSslContexts.forClient()
                                            .trustManager(trustManager)
                                            .build();
            } catch (SSLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            builder.sslContext(sslContext);
        }
    }

    public static void checkShutdownNeeded(String name, int port, Throwable throwable) {
        if( throwable instanceof StatusRuntimeException) {
            StatusRuntimeException sre = (StatusRuntimeException)throwable;
            if( sre.getStatus().getCode().equals(Status.Code.UNAVAILABLE) || sre.getStatus().getCode().equals(Status.Code.INTERNAL)) {
                ManagedChannel channel = channelPerNode.remove(channelKey(name, port));
                if( channel != null) {
                    try {
                        channel.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

            }
        }
    }
}
