package io.axoniq.axonserver;

import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.ApplicationContext;
import io.axoniq.axonserver.access.jpa.ApplicationContextRole;
import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by marc on 7/17/2017.
 */
@Component
public class AxonServerStandardAccessController implements AxonServerAccessController {

    private PathMappingRepository pathMappingRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Application dummyApplication = new Application("Dummy", null, null, null,
                                                                 new ApplicationContext("ADMIN",
                                                                                        Collections.singletonList(
                                                                                                new ApplicationContextRole(Topology.DEFAULT_CONTEXT))));

    public AxonServerStandardAccessController(PathMappingRepository pathMappingRepository, MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.pathMappingRepository = pathMappingRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public boolean allowed(String fullMethodName, String context, String token) {
        return isTokenFromConfigFile(token);
    }

    @Override
    public boolean validToken(String token) {
        return isTokenFromConfigFile(token);
    }

    @Override
    public Collection<PathMapping> getPathMappings() {
        return pathMappingRepository.findAll();
    }

    @Override
    public boolean isRoleBasedAuthentication() {
        return false;
    }

    @Override
    public Application getApplication(String token) {
        return isTokenFromConfigFile(token) ? dummyApplication: null;
    }

    private boolean isTokenFromConfigFile(String token) {
        return messagingPlatformConfiguration.getAccesscontrol().getToken().equals(token);
    }
}
