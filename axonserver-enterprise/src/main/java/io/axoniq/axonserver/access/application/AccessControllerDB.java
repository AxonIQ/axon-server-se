package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.PathToFunction;
import io.axoniq.axonserver.access.roles.FunctionRoleRepository;
import io.axoniq.axonserver.access.roles.PathToFunctionRepository;
import io.axoniq.axonserver.exception.InvalidTokenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Retrieves applications from context_application table and validates access to specific resources.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Controller
public class AccessControllerDB {

    private final Logger logger = LoggerFactory.getLogger(AccessControllerDB.class);
    private final JpaContextApplicationRepository contextApplicationRepository;
    private final PathToFunctionRepository pathToFunctionRepository;
    private final FunctionRoleRepository functionRoleRepository;
    private final Hasher hasher;
    private final SystemTokenProvider systemTokenProvider;

    /**
     * Creates the object.
     *
     * @param contextApplicationRepository repository of contexts available on this node
     * @param pathToFunctionRepository     maps path to logical function name
     * @param functionRoleRepository       maps function name to role
     * @param hasher                       hasher to hash the provided token
     * @param systemTokenProvider          provider of the system token
     */
    public AccessControllerDB(JpaContextApplicationRepository contextApplicationRepository,
                              PathToFunctionRepository pathToFunctionRepository,
                              FunctionRoleRepository functionRoleRepository,
                              Hasher hasher,
                              SystemTokenProvider systemTokenProvider) {
        this.contextApplicationRepository = contextApplicationRepository;
        this.pathToFunctionRepository = pathToFunctionRepository;
        this.functionRoleRepository = functionRoleRepository;
        this.hasher = hasher;
        this.systemTokenProvider = systemTokenProvider;
    }

    /**
     * Retrieves all roles for the token on all contexts available on this node. Throws exception if no applications
     * with specified token found.
     *
     * @param token the application token
     * @return set of ROLE@Context values
     */
    public Set<String> getRoles(String token) {
        if (token != null && token.equals(systemTokenProvider.get())) {
            return Collections.singleton("ADMIN@_admin");
        }
        Set<JpaContextApplication> apps = contextApplicationRepository.findAllByTokenPrefix(ApplicationController
                                                                                                    .tokenPrefix(token))
                                                                      .stream()
                                                                      .filter(app -> hasher
                                                                              .checkpw(token, app.getHashedToken()))
                                                                      .collect(Collectors.toSet());

        if (apps.isEmpty()) {
            throw new InvalidTokenException();
        }
        return apps.stream().map(JpaContextApplication::getQualifiedRoles)
                   .flatMap(Set::stream)
                   .collect(Collectors.toSet());
    }

    private Set<String> getRolesForContext(String context, String token) {
        if (token != null && token.equals(systemTokenProvider.get())) {
            return Collections.singleton("ADMIN@_admin");
        }
        return contextApplicationRepository.findAllByTokenPrefixAndContext(ApplicationController.tokenPrefix(token),
                                                                           context)
                                           .stream()
                                           .filter(app -> hasher.checkpw(token, app.getHashedToken()))
                                           .findFirst()
                                           .map(JpaContextApplication::getQualifiedRoles)
                                           .orElse(null);
    }

    /**
     * Checks if the app identified by the token is allowed to perform the requested operation in the specified context.
     * @param token the app token
     * @param context the current context of the connection
     * @param path the operation to authorize
     * @return true if the app identified by the token is allowed to perform the requested operation in the specified context
     */
    public boolean authorize(String token, String context, String path) {
        Set<String> requiredRoles = rolesForOperation(path);
        if (requiredRoles == null || requiredRoles.isEmpty()) {
            return true;
        }

        Set<String> roles = getRolesForContext(context, token);
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

    /**
     * Returns the roles that contain the speficied operation (based on its path).
     *
     * @param path the path for the operation
     * @return the roles that contain this operation
     */
    public Set<String> rolesForOperation(String path) {
        PathToFunction mapping = pathToFunctionRepository.findById(path)
                                                         .orElseGet(() -> findByPattern(path));
        if (mapping == null) {
            return Collections.emptySet();
        }
        return functionRoleRepository.findByFunction(mapping.getFunction())
                                     .stream()
                                     .map(functionRole -> functionRole.getRole().getRole())
                                     .collect(Collectors.toSet());
    }

}
