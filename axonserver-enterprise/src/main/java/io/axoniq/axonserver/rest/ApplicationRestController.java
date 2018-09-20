package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.cluster.events.ApplicationSynchronizationEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.grpc.ProtoConverter;
import io.axoniq.axonserver.grpc.internal.Action;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.application.ApplicationNotFoundException;
import io.axoniq.platform.application.ApplicationWithToken;
import io.axoniq.platform.role.Role;
import io.axoniq.platform.role.RoleController;
import org.springframework.context.ApplicationEventPublisher;
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
    private final ApplicationEventPublisher eventPublisher;


    public ApplicationRestController(ApplicationController applicationController,
                                     RoleController roleController, FeatureChecker limits,
                                     ApplicationEventPublisher eventPublisher) {
        this.applicationController = applicationController;
        this.roleController = roleController;
        this.limits = limits;
        this.eventPublisher = eventPublisher;
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
        ApplicationWithToken result = applicationController.updateJson(application.toApplication());
        eventPublisher.publishEvent(new ApplicationSynchronizationEvents.ApplicationReceived(
                ProtoConverter.createApplication(result.getApplication(), Action.MERGE),
                 false));
        return result.getTokenString();
    }

    private void checkRoles(ApplicationJSON application) {
        Set<String> validRoles = roleController.listApplicationRoles().stream().map(Role::name).collect(Collectors.toSet());
        for (ApplicationJSON.ApplicationRoleJSON role : application.getRoles()) {
            if( ! validRoles.contains(role.getRole())) throw new MessagingPlatformException(ErrorCode.UNKNOWN_ROLE, role.getRole() + ": Role unknown");
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
            applicationController.delete(name);
            eventPublisher.publishEvent(new ApplicationSynchronizationEvents.ApplicationReceived(
                    io.axoniq.axonserver.grpc.internal.Application.newBuilder()
                                                               .setName(name)
                    .setAction(Action.DELETE)
                    .build(), false));

        } catch(ApplicationNotFoundException notFoundException) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION, notFound(name));
        }
    }

    @PatchMapping("applications/{name}")
    public String renewToken(@PathVariable("name") String name) {
        checkEdition();
        try {
            ApplicationWithToken result = applicationController.updateToken(name);
            eventPublisher.publishEvent(new ApplicationSynchronizationEvents.ApplicationReceived(
                    ProtoConverter.createApplication(result.getApplication(), Action.MERGE),
                    false));
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
