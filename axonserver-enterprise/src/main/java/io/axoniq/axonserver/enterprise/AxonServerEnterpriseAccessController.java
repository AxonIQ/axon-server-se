package io.axoniq.axonserver.enterprise;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.platform.application.AccessController;
import io.axoniq.platform.application.PathMappingRepository;
import io.axoniq.platform.application.jpa.Application;
import io.axoniq.platform.application.jpa.ApplicationRole;
import io.axoniq.platform.application.jpa.PathMapping;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * Created by marc on 7/17/2017.
 */
@Primary
@Component
public class AxonServerEnterpriseAccessController implements AxonServerAccessController {
    private final AccessController accessController;
    private final PathMappingRepository pathMappingRepository;

    public AxonServerEnterpriseAccessController(AccessController accessController,
                                                PathMappingRepository pathMappingRepository
                                                ) {
        this.accessController = accessController;
        this.pathMappingRepository = pathMappingRepository;
    }

    @Override
    public boolean allowed(String fullMethodName, String context, String token) {
        return accessController.authorize(token, context, fullMethodName, true);
    }

    @Override
    public boolean validToken(String token) {
        return accessController.validToken(token);
    }

    @Override
    public Collection<PathMapping> getPathMappings() {
        return pathMappingRepository.findAll();
    }

    @Override
    public boolean isRoleBasedAuthentication() {
        return true;
    }

    @Override
    public Application getApplication(String token) {
        return accessController.getApplicationByToken(token);
    }

}
