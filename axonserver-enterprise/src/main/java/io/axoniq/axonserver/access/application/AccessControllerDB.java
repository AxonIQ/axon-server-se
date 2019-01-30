package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
import org.springframework.stereotype.Controller;

import java.util.Optional;

import static io.axoniq.axonserver.access.application.ApplicationController.PREFIX_LENGTH;

/**
 * @author Marc Gathier
 */
@Controller
public class AccessControllerDB {

    private final ApplicationRepository applicationRepository;
    private final PathMappingRepository pathMappingRepository;
    private final Hasher hasher;

    public AccessControllerDB(ApplicationRepository applicationRepository, PathMappingRepository pathMappingRepository, Hasher hasher) {
        this.applicationRepository = applicationRepository;
        this.pathMappingRepository = pathMappingRepository;
        this.hasher = hasher;
    }

    public boolean validToken(String token) {
        Optional<Application> applicationOptional = applicationRepository.findAll().stream()
                                                                         .filter(app -> hasher.checkpw(token, app.getHashedToken())).findFirst();
        return applicationOptional.isPresent();
    }

    public Application getApplicationByToken(String token) {
        return applicationRepository.findAll()
                .stream()
                .filter(app -> hasher.checkpw(token, app.getHashedToken()))
                .findFirst().orElse(null);

    }

    public boolean authorize(String token, String context, String path, boolean fineGrainedAccessControl) {

        String prefix = token.substring(0, Math.min(PREFIX_LENGTH, token.length()));
        Optional<Application> applicationOptional = applicationRepository.findAllByTokenPrefix(prefix)
                                                                         .stream().filter(app -> hasher.checkpw(token, app.getHashedToken())).findFirst();

        if (!applicationOptional.isPresent()) {
            // Not found based on prefix, check applications without prefix (Migration from AxonDB creates applications without prefix as AxonDB does not store prefix)
            applicationOptional = applicationRepository.findAllByTokenPrefix(null).stream().filter(app -> hasher
                    .checkpw(token, app.getHashedToken())).findFirst();
        }

        if (!applicationOptional.isPresent())
            return false;

        if( !fineGrainedAccessControl) return true;

        PathMapping mapping = pathMappingRepository.findById(path).orElseGet(() -> findByPrefix(path));
        return mapping != null && applicationOptional.get().hasRoleForContext(mapping.getRole(), context);
    }

    private PathMapping findByPrefix(String path) {
        return pathMappingRepository.findAll().stream().filter(m -> m.getPath().endsWith("*"))
                                    .filter(m -> path.startsWith(m.getPath().substring(0, m.getPath().length() -1)))
                                    .findFirst().orElse(null);
    }
}
