package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * @author Marc Gathier
 * Retrieves applications from context_application table and validates access to specific resources.
 */
@Controller
public class AccessControllerDB {

    private final JpaContextApplicationRepository applicationRepository;
    private final PathMappingRepository pathMappingRepository;
    private final Hasher hasher;

    public AccessControllerDB(JpaContextApplicationRepository applicationRepository, PathMappingRepository pathMappingRepository, Hasher hasher) {
        this.applicationRepository = applicationRepository;
        this.pathMappingRepository = pathMappingRepository;
        this.hasher = hasher;
    }

    public boolean validToken(String token) {
        Optional<JpaContextApplication> applicationOptional = applicationRepository.findAll().stream()
                                                                                   .filter(app -> hasher.checkpw(token, app.getHashedToken())).findFirst();
        return applicationOptional.isPresent();
    }

    public Set<String> getAdminRoles(String token) {
        return getRoles(token, getAdmin());
    }

    private Set<String> getRoles(String token, String group) {
        return applicationRepository.findAllByContext(group)
                                    .stream()
                                    .filter(app -> hasher.checkpw(token, app.getHashedToken()))
                                    .findFirst()
                                    .map(JpaContextApplication::getRoles)
                                    .orElse(null);
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

    public Collection<PathMapping> getPathMappins() {
        return pathMappingRepository.findAll();
    }
}
