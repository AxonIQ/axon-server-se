package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.access.application.ApplicationContextRole;
import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.ApplicationNotFoundException;
import io.axoniq.axonserver.access.role.RoleController;
import io.axoniq.axonserver.access.roles.Role;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.licensing.Feature;
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
import java.util.stream.Collectors;

/**
 * Created by marc on 7/14/2017.
 */
@RestController("ApplicationRestController")
@CrossOrigin
@RequestMapping("/v1")
public class ApplicationRestController {

    private static final String ACTION_NOT_SUPPORTED_IN_DEVELOPMENT_MODE = "Action not supported in Standard Edition";
    private static final String APPLICATION_NOT_FOUND = "JpaApplication %s not found";
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
    public String updateJson(@RequestBody ApplicationJSON application) {
        checkEdition();
        checkRoles(application);
        Application savedApplication = raftServiceFactory.getRaftConfigService()
                                 .updateApplication(ApplicationProtoConverter.createApplication(application));
        return savedApplication.getToken();
    }

    private void checkRoles(ApplicationJSON application) {
        Set<String> validRoles = roleController.listRoles()
                                               .stream()
                                               .map(Role::getRole)
                                               .collect(Collectors.toSet());
        List<String> roles = application.getRoles()
                                        .stream()
                                        .map(r -> r.toApplicationRole().getRoles())
                                        .flatMap(List::stream)
                                        .map(ApplicationContextRole::getRole)
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
    public void delete(@PathVariable("name") String name) {
        checkEdition();
        try {
            raftServiceFactory.getRaftConfigService().deleteApplication(Application.newBuilder().setName(name).build());
        } catch(ApplicationNotFoundException notFoundException) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION, notFound(name));
        }
    }

    @PatchMapping("applications/{name}")
    public String renewToken(@PathVariable("name") String name) {
        checkEdition();
        try {
            Application savedApplication =  raftServiceFactory.getRaftConfigService()
                                     .refreshToken(Application.newBuilder().setName(name).build());
            return savedApplication.getToken();
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
