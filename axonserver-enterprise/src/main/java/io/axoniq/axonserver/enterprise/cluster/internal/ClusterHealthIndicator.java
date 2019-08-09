package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marc Gathier
 */
@Component
public class ClusterHealthIndicator extends AbstractHealthIndicator {

    private final MessagingClusterService messagingClusterService;
    private final ClusterController clusterController;

    public ClusterHealthIndicator(MessagingClusterService messagingClusterService, ClusterController clusterController) {
        this.messagingClusterService = messagingClusterService;
        this.clusterController = clusterController;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        AtomicBoolean anyConnectionUp = new AtomicBoolean();
        clusterController.getRemoteConnections().forEach(rc -> {
            boolean connected = rc.isConnected();
            if (!connected) {
                builder.status(new Status("WARN"));
            } else {
                anyConnectionUp.set(true);
            }
            builder.withDetail(String.format("%s.connection", rc.getClusterNode().getName()), connected ? "UP" : "DOWN");
        });
        messagingClusterService.listeners().forEach(listener -> {
            if (listener instanceof GrpcInternalCommandDispatcherListener) {
                if (listener.waiting() > 0) {
                    builder.status(new Status("WARN"));
                }
                builder.withDetail(String.format("%s.commands.waiting", listener.queue()), listener.waiting());
                builder.withDetail(String.format("%s.commands.permits", listener.queue()), listener.permits());
            }
            if (listener instanceof GrpcInternalQueryDispatcherListener) {
                if (listener.waiting() > 0) {
                    builder.status(new Status("WARN"));
                }
                builder.withDetail(String.format("%s.queries.waiting", listener.queue()), listener.waiting());
                builder.withDetail(String.format("%s.queries.permits", listener.queue()), listener.permits());
            }
        });
        if (!anyConnectionUp.get() && !clusterController.getRemoteConnections().isEmpty()) {
            builder.down();
        }
    }
}
