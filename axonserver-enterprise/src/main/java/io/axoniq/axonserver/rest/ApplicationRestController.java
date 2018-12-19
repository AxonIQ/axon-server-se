package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.rest.json.ApplicationJSON;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.application.ApplicationNotFoundException;
import io.axoniq.platform.application.ApplicationWithToken;
import io.axoniq.platform.role.Role;
import io.axoniq.platform.role.RoleController;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Created by marc on 7/14/2017.
 */
@RestController("ApplicationRestController")
@CrossOrigin
@RequestMapping("/v1")
@Transactional
public class ApplicationRestController {

    public static final String ACTION_NOT_SUPPORTED_IN_DEVELOPMENT_MODE = "Action not supported in Development mode";
    public static final String APPLICATION_NOT_FOUND = "Application %s not found";
    private final ApplicationController applicationController;
    private final RoleController roleController;
    private final FeatureChecker limits;
    private final RaftConfigServiceFactory raftServiceFactory;


    public ApplicationRestController(ApplicationController applicationController,
                                     RoleController roleController, FeatureChecker limits,
                                     RaftConfigServiceFactory raftServiceFactory
                                     ) {
        this.applicationController = applicationController;
        this.roleController = roleController;
        this.limits = limits;
        this.raftServiceFactory = raftServiceFactory;
    }

    @GetMapping("public/applications")
    public List<ApplicationJSON> getApplications() {
        checkEdition();
        return applicationController.getApplications().stream().map(ApplicationJSON::new).collect(Collectors.toList());
    }

    @PostMapping("applications")
    public String updateJson(@RequestBody ApplicationJSON application) throws ExecutionException, InterruptedException {
        checkEdition();
        checkRoles(application);
        if( application.getToken() == null ) {
            try{
                applicationController.get(application.getName());
            } catch (ApplicationNotFoundException notFound) {
                application.setToken(UUID.randomUUID().toString());
            }
        }

        raftServiceFactory.getRaftConfigService().updateApplication(application.asProto()).get();
        return application.getToken();
    }

    private void checkRoles(ApplicationJSON application) {
        Set<String> validRoles = roleController.listApplicationRoles()
                                               .stream()
                                               .map(Role::name)
                                               .collect(Collectors.toSet());
        List<String> roles = application.getRoles()
                                        .stream()
                                        .map(ApplicationJSON.ApplicationRoleJSON::getRoles)
                                        .flatMap(List::stream)
                                        .distinct()
                                        .collect(Collectors.toList());
        for (String role : roles) {
            if (!validRoles.contains(role)) {
                throw new MessagingPlatformException(ErrorCode.UNKNOWN_ROLE,
                                                     role + ": Role unknown");
            }
        }
    }


    @GetMapping("applications/{name}")
    public ApplicationJSON get(@PathVariable("name") String name) {
        checkEdition();
        try {
            return new ApplicationJSON(applicationController.get(name));
        } catch(ApplicationNotFoundException notFoundException) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION, notFound(name));
        }

    }

    @DeleteMapping("applications/{name}")
    public CompletableFuture<Void> delete(@PathVariable("name") String name) {
        checkEdition();
        try {
            return raftServiceFactory.getRaftConfigService().deleteApplication(Application.newBuilder().setName(name).build());
        } catch(ApplicationNotFoundException notFoundException) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION, notFound(name));
        }
    }

    @PatchMapping("applications/{name}")
    public String renewToken(@PathVariable("name") String name) {
        checkEdition();
        try {
            ApplicationWithToken result = applicationController.updateToken(name);
            return result.getTokenString();
        } catch(ApplicationNotFoundException notFoundException) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION, notFound(name));
        }
    }

    private void checkEdition() {
        if (!Feature.APP_AUTHENTICATION.enabled(limits))
            throw new MessagingPlatformException(ErrorCode.NOT_SUPPORTED_IN_DEVELOPMENT,
                                                 ACTION_NOT_SUPPORTED_IN_DEVELOPMENT_MODE);
    }

    private String notFound(String name) {
        return String.format(APPLICATION_NOT_FOUND, name);
    }

}
