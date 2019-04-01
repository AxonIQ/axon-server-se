package io.axoniq.axonserver.migration;

import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class AxonServerProperty {
    private final String name;
    private final Function<String, String> legacyDefault;

    public AxonServerProperty(String name, String legacyDefault) {
        this.name = name;
        this.legacyDefault = val -> val == null ? legacyDefault : val;
    }
    public AxonServerProperty(String name) {
        this.name = name;
        this.legacyDefault = null;
    }

    public AxonServerProperty(String name, Function<String, String> legacyDefault) {
        this.name = name;
        this.legacyDefault = legacyDefault;
    }

    public String getName() {
        return name;
    }

    public String legacyDefault(String legacyValue) {
        return legacyDefault != null ? legacyDefault.apply(legacyValue) : legacyValue;
    }

    public boolean hasLegacyDefault() {
        return legacyDefault != null;
    }
}
