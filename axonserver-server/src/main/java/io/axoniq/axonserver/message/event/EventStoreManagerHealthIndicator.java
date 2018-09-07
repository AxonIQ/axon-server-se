package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.context.ContextController;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
@ConditionalOnProperty(value = "axoniq.axonhub.cluster.enabled", havingValue = "true")
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
