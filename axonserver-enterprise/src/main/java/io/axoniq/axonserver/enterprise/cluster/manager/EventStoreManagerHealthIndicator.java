package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.enterprise.context.ContextController;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Author: marc
 */
@Component
@ConditionalOnBean(EventStoreManager.class)
class EventStoreManagerHealthIndicator extends AbstractHealthIndicator {

    private final ContextController contextController;
    private EventStoreManager eventStoreManager;

    public EventStoreManagerHealthIndicator(ContextController contextController, EventStoreManager eventStoreManager) {
        this.contextController = contextController;
        this.eventStoreManager = eventStoreManager;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        builder.up();
        contextController.getContexts().forEach(c -> {
            String master = eventStoreManager.getMaster(c.getName());
            if (master != null) {
                builder.withDetail(c.getName(), master);
            } else {
                builder.withDetail(c.getName(), "Unknown");
                builder.unknown();
            }
        });
    }
}
