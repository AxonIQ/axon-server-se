package io.axoniq.axonserver.enterprise;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.access.application.AccessController;
import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
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
