package io.axoniq.axonserver.grpc;


import io.axoniq.axonserver.access.application.ApplicationContext;
import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.rest.ApplicationJSON;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class ApplicationProtoConverter {
    public static ApplicationContextRole createApplicationContextRole(
            ApplicationJSON.ApplicationRoleJSON applicationContext) {
        applicationContext.toApplicationRole();
        List<String> roles = applicationContext.toApplicationRole().getRoles()
                                               .stream()
                                               .map(io.axoniq.axonserver.access.application.ApplicationContextRole::getRole)
                                               .collect(Collectors.toList());
        return ApplicationContextRole.newBuilder()
                                     .setContext(applicationContext.getContext())
                                     .addAllRoles(roles)
                                     .build();
    }

    public static Application createApplication(ApplicationJSON app) {
        Application.Builder builder = Application.newBuilder().setName(app.getName());
        if (app.getDescription() != null) {
            builder.setDescription(app.getDescription());
        }
        if( app.getToken() != null) {
            builder.setToken(app.getToken());
        }
        app.getRoles().stream()
           .map(ApplicationProtoConverter::createApplicationContextRole)
           .forEach(builder::addRolesPerContext);
        return builder.build();
    }

    public static Application createApplication(JpaApplication app) {
        Application.Builder builder = Application.newBuilder().setName(app.getName());
        if (app.getDescription() != null) {
            builder.setDescription(app.getDescription());
        }
        if (app.getHashedToken() != null) {
            builder.setToken(app.getHashedToken());
        }
        if (app.getTokenPrefix() != null) {
            builder.setTokenPrefix(app.getTokenPrefix());
        }
        app.getContexts()
           .stream()
           .map(ApplicationProtoConverter::createApplicationContextRole)
           .forEach(builder::addRolesPerContext);
        return builder.build();
    }

    public static ApplicationContextRole createApplicationContextRole(ApplicationContext applicationContext) {
        List<String> roles = applicationContext.getRoles()
                                               .stream()
                                               .map(io.axoniq.axonserver.access.application.ApplicationContextRole::getRole)
                                               .collect(Collectors.toList());
        return ApplicationContextRole.newBuilder()
                                     .setContext(applicationContext.getContext())
                                     .addAllRoles(roles)
                                     .build();
    }



    public static JpaApplication createJpaApplication(Application application) {
        List<ApplicationContext> applicationContexts = application.getRolesPerContextList()
                                                                  .stream()
                                                                  .map(ApplicationProtoConverter::createJpaApplicationContext)
                                                                  .collect(Collectors.toList());

        return new JpaApplication(application.getName(),
                                                                  application.getDescription(),
                                                                  application.getTokenPrefix(),
                                                                  application.getToken(),
                                                                  applicationContexts);
    }

    public static ApplicationContext createJpaApplicationContext(ApplicationContextRole applicationContextRole) {
        List<io.axoniq.axonserver.access.application.ApplicationContextRole> roles =
                applicationContextRole.getRolesList()
                                      .stream()
                                      .map(io.axoniq.axonserver.access.application.ApplicationContextRole::new)
                                      .collect(Collectors.toList());

        return new ApplicationContext(applicationContextRole.getContext(), roles);
    }


}
