package io.axoniq.axonserver.config;

import org.springframework.boot.actuate.health.Status;

/**
 * Utility that hold custom status codes to be used by actuator health.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public abstract class HealthStatus {

    public static final Status WARN_STATUS = new Status("WARN");
}
