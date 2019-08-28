package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.PathToFunction;
import io.axoniq.axonserver.access.roles.FunctionRoleRepository;
import io.axoniq.axonserver.access.roles.PathToFunctionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;

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
    @Value("${axoniq.axonserver.accesscontrol.token-dir:security}")
    private String systemTokenDir = "security";

    private final String[] systemToken = new String[1];


    public AccessControllerDB(JpaContextApplicationRepository contextApplicationRepository,
                              PathToFunctionRepository pathToFunctionRepository,
                              FunctionRoleRepository functionRoleRepository,
                              Hasher hasher) {
        this.contextApplicationRepository = contextApplicationRepository;
        this.pathToFunctionRepository = pathToFunctionRepository;
        this.functionRoleRepository = functionRoleRepository;
        this.hasher = hasher;
    }

    /**
     * Retrieves all roles for the token on all contexts available on this node.
     *
     * @param token the application token
     * @return set of ROLE@Context values
     */
    public Set<String> getRoles(String token) {
        if (token != null && token.equals(systemToken[0])) {
            return Collections.singleton("ADMIN@_admin");
        }
        return contextApplicationRepository.findAllByTokenPrefix(ApplicationController.tokenPrefix(token))
                                           .stream()
                                           .filter(app -> hasher.checkpw(token, app.getHashedToken()))
                                           .map(JpaContextApplication::getQualifiedRoles)
                                           .flatMap(Set::stream)
                                           .collect(Collectors.toSet());
    }

    private Set<String> getRolesForContext(String context, String token) {
        if (token != null && token.equals(systemToken[0])) {
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
        Set<String> requiredRoles = getPathMappings(path);
        if (requiredRoles == null || requiredRoles.isEmpty()) {
            return true;
        }

        Set<String> roles = getRolesForContext(context, token);
        logger.warn("Authorizing {}: required roles: {}, app roles: {}, context: {}", path,
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


    public String systemToken() {
        return systemToken[0];
    }

    @PostConstruct
    void generateSystemToken() {
        File securityDir = new File(systemTokenDir);
        if (!securityDir.exists()) {
            if (!securityDir.mkdirs()) {
                logger.warn("Cannot create directory for system token: {}", securityDir);
                return;
            }
        }

        try (FileWriter tokenFile = new FileWriter(new File(
                securityDir.getAbsolutePath() + File.separator + ".token"))) {
            systemToken[0] = UUID.randomUUID().toString();
            tokenFile.write(systemToken[0]);
            tokenFile.write('\n');
        } catch (IOException ioException) {
            logger.warn("Cannot write file for system token in: {}", securityDir, ioException);
        }
    }


}
