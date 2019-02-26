package io.axoniq.axonserver.enterprise.cluster.internal;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class ClusterHealthIndicator extends AbstractHealthIndicator {

    private final MessagingClusterService messagingClusterService;

    public ClusterHealthIndicator(MessagingClusterService messagingClusterService) {
        this.messagingClusterService = messagingClusterService;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        messagingClusterService.listeners().forEach(listener-> {
            if( listener instanceof GrpcInternalCommandDispatcherListener) {
                builder.withDetail(String.format("%s.commands.waiting", listener.queue()), listener.waiting());
                builder.withDetail(String.format("%s.commands.permits", listener.queue()), listener.permits());
            }
            if( listener instanceof GrpcInternalQueryDispatcherListener) {
                builder.withDetail(String.format("%s.queries.waiting", listener.queue()), listener.waiting());
                builder.withDetail(String.format("%s.queries.permits", listener.queue()), listener.permits());
            }
        });

    }
}
