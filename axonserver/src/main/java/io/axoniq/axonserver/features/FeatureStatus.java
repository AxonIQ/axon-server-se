package io.axoniq.axonserver.features;


import io.axoniq.axonserver.KeepNames;

/**
 * @author Marc Gathier
 */
@KeepNames
public class FeatureStatus {
    private final Feature name;
    private final boolean enabled;

    public FeatureStatus(Feature name, boolean enabled) {
        this.name = name;
        this.enabled = enabled;
    }

    public Feature getName() {
        return name;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
