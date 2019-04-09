package io.axoniq.axonserver.migration;

import java.util.function.Function;

/**
 * Data class containing an Axon Server property specifying the {@code name} of the property and a {@link Function} to
 * map a {@code legacyDefault}.
 *
 * @author Marc Gathier
 * @since 4.1
 */
class AxonServerProperty {

    private final String name;
    private final Function<String, String> legacyDefault;

    /**
     * Instantiate a {@link AxonServerProperty} with the given {@code name} and {@code legacyDefault}.
     *
     * @param name          a {@link String} specifying the name of property
     * @param legacyDefault a {@link String} specifying the legacy default of the property
     */
    AxonServerProperty(String name, String legacyDefault) {
        this.name = name;
        this.legacyDefault = val -> val == null ? legacyDefault : val;
    }

    /**
     * Instantiate a {@link AxonServerProperty} with the given {@code name} and a default of {@code null}.
     *
     * @param name a {@link String} specifying the name of property
     */
    AxonServerProperty(String name) {
        this.name = name;
        this.legacyDefault = null;
    }

    /**
     * Instantiate a {@link AxonServerProperty} with the given {@code name} and {@code legacyDefault} mapping {@code
     * Function}.
     *
     * @param name          a {@link String} specifying the name of property
     * @param legacyDefault a {@link Function} from {@link String} to String which will provide the legacy default of
     *                      the property
     */
    AxonServerProperty(String name, Function<String, String> legacyDefault) {
        this.name = name;
        this.legacyDefault = legacyDefault;
    }

    /**
     * Retrieve the {@code name} of this Axon Server Property
     *
     * @return a {@link String} defining the {@code name} of this Axon Server Property
     */
    String getName() {
        return name;
    }

    /**
     * Retrieve the legacy default of this Axon Server property, using the given {@code legacyValue} to map the old
     * value to the new value
     *
     * @param legacyValue a {@link String} defining the old value for this property
     * @return a {@link String} defining the new default of this property
     */
    String legacyDefault(String legacyValue) {
        return legacyDefault != null ? legacyDefault.apply(legacyValue) : legacyValue;
    }

    /**
     * Check whether this Axon Server property has a legacy default.
     *
     * @return {@code true} if there is a default and {@code false} if there isn't
     */
    boolean hasLegacyDefault() {
        return legacyDefault != null;
    }
}
