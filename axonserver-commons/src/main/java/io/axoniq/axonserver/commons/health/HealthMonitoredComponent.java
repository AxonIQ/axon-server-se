package io.axoniq.axonserver.commons.health;

import java.util.Map;

/**
 * Interface that each component that publish own health check
 * should implement
 *
 * @author Stefan Dragisic
 */
public interface HealthMonitoredComponent {

    enum Status {
        UP("UP"),
        DOWN("DOWN"),
        WARN("WARN"),
        IGNORE("IGNORE");

        public final String label;

        Status(String label) {
            this.label = label;
        }
    }

    interface Health {
        Status status();
        Map<String,String> details();
    }

    String healthCategory();
    Health health();

}
