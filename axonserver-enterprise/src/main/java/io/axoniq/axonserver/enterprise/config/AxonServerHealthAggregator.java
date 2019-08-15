package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.enterprise.HealthStatus;
import org.springframework.boot.actuate.health.OrderedHealthAggregator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * Aggregator to determine the overall health of the server. Adds logic for setting status to WARN if one of the
 * parts is in WARN status.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Component
@Primary
public class AxonServerHealthAggregator extends OrderedHealthAggregator {

    public AxonServerHealthAggregator() {
        setStatusOrder(Status.DOWN, Status.OUT_OF_SERVICE, HealthStatus.WARN_STATUS,
                       Status.UP, Status.UNKNOWN);
    }
}
