package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.roles.FunctionRoleRepository;
import io.axoniq.axonserver.access.jpa.PathToFunction;
import io.axoniq.axonserver.access.roles.PathToFunctionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * Retrieves applications from context_application table and validates access to specific resources.
 */
@Controller
public class AccessControllerDB {

    private final Logger logger = LoggerFactory.getLogger(AccessControllerDB.class);
    private final JpaContextApplicationRepository contextApplicationRepository;
    private final PathToFunctionRepository pathToFunctionRepository;
    private final FunctionRoleRepository functionRoleRepository;
    private final Hasher hasher;

    public AccessControllerDB(JpaContextApplicationRepository contextApplicationRepository,
                              PathToFunctionRepository pathToFunctionRepository,
                              FunctionRoleRepository functionRoleRepository,
                              Hasher hasher) {
        this.contextApplicationRepository = contextApplicationRepository;
        this.pathToFunctionRepository = pathToFunctionRepository;
        this.functionRoleRepository = functionRoleRepository;
        this.hasher = hasher;
    }

    public Set<String> getRoles(String token) {
        return contextApplicationRepository.findAllByTokenPrefix(ApplicationController.tokenPrefix(token))
                                           .stream()
                                           .filter(app -> hasher.checkpw(token, app.getHashedToken()))
                                           .findFirst()
                                           .map(JpaContextApplication::getQualifiedRoles)
                                           .orElse(null);
    }

    public boolean authorize(String token, String context, String path) {
        Set<String> requiredRoles = getPathMappings(path);
        if (requiredRoles == null || requiredRoles.isEmpty()) {
            return true;
        }

        Set<String> roles = getRoles(token);
        logger.debug("Authorizing {}: required roles: {}, app roles: {}, context: {}", path,
                     requiredRoles,
                     roles,
                     context);
        if (roles == null)
            return false;

        return requiredRoles.stream().anyMatch(role -> roles.contains(role + '@' + context) || roles
                .contains(role + "@*"));
    }

    private PathToFunction findByPattern(String path) {
        return pathToFunctionRepository.findAll().stream()
                                       .filter(m -> Pattern.compile(m.getPath()).matcher(path).matches())
                                    .findFirst().orElse(null);
    }

    public Set<String> getPathMappings(String permission) {
        PathToFunction mapping = pathToFunctionRepository.findById(permission)
                                                         .orElseGet(() -> findByPattern(permission));
        if (mapping == null) {
            return Collections.emptySet();
        }
        return functionRoleRepository.findByFunction(mapping.getFunction())
                                     .stream()
                                     .map(functionRole -> functionRole.getRole().getRole())
                                     .collect(Collectors.toSet());
    }

}
