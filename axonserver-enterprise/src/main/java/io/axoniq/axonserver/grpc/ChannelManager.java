package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;

/**
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


    @Override
    public Channel get(String hostname, int port) {
        ChannelKey channelKey = new ChannelKey(hostname, port);
        channels.computeIfAbsent(channelKey, key -> ManagedChannelHelper
                .createManagedChannel(configuration, hostname, port));
        return channels.get(channelKey);
    }

    @PostConstruct
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
}
