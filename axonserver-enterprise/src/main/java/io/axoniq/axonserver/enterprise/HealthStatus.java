package io.axoniq.axonserver.enterprise;

import org.springframework.boot.actuate.health.Status;

/**
 * Utility that hold custom status codes to be used by actuator health. Add these in {@link
 * io.axoniq.axonserver.enterprise.config.AxonServerHealthAggregator} to
 * ensure that the calculation of the aggregated status is correct.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public abstract class HealthStatus {

    public static final Status WARN_STATUS = new Status("WARN");
}
