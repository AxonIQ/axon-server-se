package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.GrpcBufferingInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

/**
 * @author Marc Gathier
 */
public class ManagedChannelHelper {
    private static final Logger logger = LoggerFactory.getLogger(ManagedChannelHelper.class);

    public static ManagedChannel createManagedChannel(ExecutorService executors,
                                                      MessagingPlatformConfiguration messagingPlatformConfiguration,
                                                      String host, int port) {
        ManagedChannel channel = null;
        try {
            NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);

            if (messagingPlatformConfiguration.getKeepAliveTime() > 0) {
                builder.keepAliveTime(messagingPlatformConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS)
                       .keepAliveTimeout(messagingPlatformConfiguration.getKeepAliveTimeout(), TimeUnit.MILLISECONDS)
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
            builder.executor(executors);
            builder.intercept(new GrpcBufferingInterceptor(messagingPlatformConfiguration.getGrpcBufferedMessages()));
            channel = builder.build();
        } catch(Exception ex) {
            logger.warn("Error while building channel - {}", ex.getMessage());
            return null;
        }
        return channel;
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

}
