package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.QueryService;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class QueryHealthIndicator extends AbstractHealthIndicator {

    private final QueryService queryService;

    public QueryHealthIndicator(QueryService queryService) {
        this.queryService = queryService;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();
        queryService.listeners().forEach(listener-> {
            builder.withDetail(String.format("%s.waitingQueries", listener.queue()), listener.waiting());
            builder.withDetail(String.format("%s.permits", listener.queue()), listener.permits());
        });

    }
}
