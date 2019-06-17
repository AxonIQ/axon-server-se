package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * Retrieves applications from context_application table and validates access to specific resources.
 */
@Controller
public class AccessControllerDB {

    private final JpaContextApplicationRepository contextApplicationRepository;
    private final JpaApplicationRepository applicationRepository;
    private final PathMappingRepository pathMappingRepository;
    private final Hasher hasher;

    public AccessControllerDB(JpaContextApplicationRepository contextApplicationRepository,
                              JpaApplicationRepository applicationRepository,
                              PathMappingRepository pathMappingRepository,
                              Hasher hasher) {
        this.contextApplicationRepository = contextApplicationRepository;
        this.applicationRepository = applicationRepository;
        this.pathMappingRepository = pathMappingRepository;
        this.hasher = hasher;
    }

    public Set<String> getRoles(String token, String group) {
        Set<String> roles = contextApplicationRepository.findAllByContext(group)
                                                        .stream()
                                                        .filter(app -> hasher.checkpw(token, app.getHashedToken()))
                                                        .findFirst()
                                                        .map(JpaContextApplication::getRoles)
                                                        .orElse(null);
        if( roles == null) {
            JpaApplication application = applicationRepository.findAllByTokenPrefix(ApplicationController.tokenPrefix(token))
                                                           .stream()
                                                           .filter(app -> hasher.checkpw(token, app.getHashedToken()))
                                                           .findFirst().orElse(null);
            if( application != null) {
                roles = application.getContexts().stream().filter(c -> c.getContext().equals(group)).flatMap(g -> g.getRoles().stream())
                                   .map(ApplicationContextRole::getRole).collect(Collectors.toSet());
            }
        }
        return roles;
    }

    public boolean authorize(String token, String context, String path, boolean fineGrainedAccessControl) {
        Set<String> roles = getRoles(token, context);
        if (roles == null)
            return false;

        if( !fineGrainedAccessControl) return true;

        PathMapping mapping = pathMappingRepository.findById(path).orElseGet(() -> findByPrefix(path));
        return mapping != null && roles.contains(mapping.getRole());
    }

    private PathMapping findByPrefix(String path) {
        return pathMappingRepository.findAll().stream().filter(m -> m.getPath().endsWith("*"))
                                    .filter(m -> path.startsWith(m.getPath().substring(0, m.getPath().length() -1)))
                                    .findFirst().orElse(null);
    }

    public Collection<PathMapping> getPathMappings() {
        return pathMappingRepository.findAll();
    }
}
