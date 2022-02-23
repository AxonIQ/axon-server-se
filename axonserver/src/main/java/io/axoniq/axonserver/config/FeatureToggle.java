package io.axoniq.axonserver.config;

/**
 * A class implementing this interface contains configuration settings for an (experimental) feature, which can
 * be enabled or disabled using the "enabled" property.
 *
 * @author Bert Laverman
 * @since 4.6.0
 */
public interface FeatureToggle {

    /**
     * Return the name of this feature. This should be a read-only property and will be the key of the properties
     * group.
     *
     * @return the name of this feature.
     */
    String getName();

    /**
     * Return {@code true} if the feature is enabled.
     *
     * @return {@code true} if the feature is enabled.
     */
    boolean isEnabled();

    /**
     * Enable or disable this feature.
     *
     * @param enabled the requested state for this feature.
     */
    void setEnabled(boolean enabled);
}
