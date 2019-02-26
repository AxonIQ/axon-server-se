package io.axoniq.axonserver;


import io.axoniq.axonserver.access.jpa.PathMapping;

import java.util.Collection;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public interface AxonServerAccessController {
    String TOKEN_PARAM = "AxonIQ-Access-Token";
    String AXONDB_TOKEN_PARAM = "Access-Token";
    String CONTEXT_PARAM = "AxonIQ-Context";

    boolean allowed(String fullMethodName, String context, String token);

    Collection<PathMapping> getPathMappings();

    boolean isRoleBasedAuthentication();

    Set<String> getAdminRoles(String token);
}
