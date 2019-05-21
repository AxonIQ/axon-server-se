package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;

/**
 * This class's goal is to centralize the creation of the {@link Channel}s, in order to have one single {@link Channel}
 * for any connection uniquely identified by hostname and port.
 *
 * As this class maintains a reference to all valid {@link Channel}s, is also responsible to shutdown properly them
 * properly if it is needed.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@Component
public class ChannelManager implements ChannelProvider, ChannelCloser {

    private final MessagingPlatformConfiguration configuration;
    private final Map<ChannelKey, ManagedChannel> channels = new ConcurrentHashMap<>();

    public ChannelManager(MessagingPlatformConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Returns the {@link Channel} to communicate with the specified server, creating a new one if it doesn't exists.
     *
     * @param hostname the hostname of the server
     * @param port     the port of the server
     * @return the channel
     */
    @Override
    public Channel get(String hostname, int port) {
        ChannelKey channelKey = new ChannelKey(hostname, port);
        channels.computeIfAbsent(channelKey, key -> ManagedChannelHelper
                .createManagedChannel(configuration, hostname, port));
        return channels.get(channelKey);
    }

    /**
     * Before to destroy this instance, it shutdowns properly all the opened {@link Channel}s
     */
    @PreDestroy
    public void shutdown() {
        channels.forEach((node, channel) -> {
            if (!channel.isShutdown()) {
                try {
                    channel.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
                    channels.remove(node, channel);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /**
     * Shutdowns the {@link Channel} with the specified AxonServer instance if it is no more usable.
     *
     * @param hostname  the hostname of the other AxonServer instance
     * @param port      the gRPC port used by the channel to connect to AxonServer instance
     * @param throwable the exception that requires the shutdown check
     * @return true if the {@link io.grpc.Channel} has been shutdown, false otherwise
     */
    @Override
    public boolean shutdownIfNeeded(String hostname, int port, Throwable throwable) {
        ChannelKey channelKey = new ChannelKey(hostname, port);
        if (throwable instanceof StatusRuntimeException) {
            StatusRuntimeException sre = (StatusRuntimeException) throwable;
            if (sre.getStatus().getCode().equals(Status.Code.UNAVAILABLE) ||
                    sre.getStatus().getCode().equals(Status.Code.INTERNAL)) {
                ManagedChannel channel = channels.remove(channelKey);
                if (channel != null) {
                    try {
                        channel.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
                        return true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        return false;
    }

    private static final class ChannelKey {

        @Nonnull private final String host;
        private final int port;

        ChannelKey(@Nonnull String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ChannelKey that = (ChannelKey) o;
            return port == that.port &&
                    host.equals(that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }
    }
}
