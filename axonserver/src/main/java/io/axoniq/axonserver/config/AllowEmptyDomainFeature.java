package io.axoniq.axonserver.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Allow for the (internal) domain to be empty. Default behavior is to copy the client domain is no internal domain is
 * set. The actual behavior is implemented in {@link MessagingPlatformConfiguration#getInternalDomain()}.
 *
 * @author Bert Laverman
 * @since 4.6.0
 */
@Component
public class AllowEmptyDomainFeature implements FeatureToggle {

    public static final String NAME = "allow-empty-domain";

    @Value("${axoniq.axonserver.experimental.allow-empty-domain.enabled:false}")
    private boolean enabled = false;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
