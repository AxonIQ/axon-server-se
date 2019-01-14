package io.axoniq.axonserver;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.platform.application.AccessController;
import io.axoniq.platform.application.PathMappingRepository;
import io.axoniq.platform.application.jpa.Application;
import io.axoniq.platform.application.jpa.PathMapping;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * @author Marc Gathier
 */
@Component
public class AxonServerAccessController {
    public static final String TOKEN_PARAM = "AxonIQ-Access-Token";
    public static final String CONTEXT_PARAM = "AxonIQ-Context";
    private final AccessController accessController;
    private final PathMappingRepository pathMappingRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final FeatureChecker limits;

    public AxonServerAccessController(AccessController accessController,
                                      PathMappingRepository pathMappingRepository,
                                      MessagingPlatformConfiguration messagingPlatformConfiguration,
                                      FeatureChecker limits) {
        this.accessController = accessController;
        this.pathMappingRepository = pathMappingRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.limits = limits;
    }

    public boolean allowed(String fullMethodName, String context, String token) {
        if(! Feature.APP_AUTHENTICATION.enabled(limits)) return messagingPlatformConfiguration.getAccesscontrol().getToken().equals(token);

        return accessController.authorize(token, context, fullMethodName, limits.isEnterprise());
    }

    public boolean validToken(String token) {
        if(! Feature.APP_AUTHENTICATION.enabled(limits))  return messagingPlatformConfiguration.getAccesscontrol().getToken().equals(token);
        return accessController.validToken(token);
    }

    public Collection<PathMapping> getPathMappings() {
        return pathMappingRepository.findAll();
    }

    public boolean isRoleBasedAuthentication() {
        return Feature.APP_AUTHENTICATION.enabled(limits);
    }

    public Application getApplication(String token) {
        if(! Feature.APP_AUTHENTICATION.enabled(limits)) {
            if( messagingPlatformConfiguration.getAccesscontrol().getToken().equals(token) ) {
                return new Application("Dummy");
            }
            return null;
        }

        return accessController.getApplicationByToken(token);
    }
}
