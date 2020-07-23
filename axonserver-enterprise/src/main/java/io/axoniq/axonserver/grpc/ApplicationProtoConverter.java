package io.axoniq.axonserver.grpc;


import io.axoniq.axonserver.access.application.AdminApplicationContext;
import io.axoniq.axonserver.access.application.AdminApplication;
import io.axoniq.axonserver.access.application.AdminApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.rest.ApplicationJSON;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class ApplicationProtoConverter {
    public static ApplicationContextRole createApplicationContextRole(
            ApplicationJSON.ApplicationRoleJSON applicationContext) {
        applicationContext.toApplicationRole();
        List<String> roles = applicationContext.toApplicationRole().getRoles()
                                               .stream()
                                               .map(AdminApplicationContextRole::getRole)
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

    public static Application createApplication(AdminApplication app) {
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
        builder.putAllMetaData(app.getMetaDataMap());
        app.getContexts()
           .stream()
           .map(ApplicationProtoConverter::createApplicationContextRole)
           .forEach(builder::addRolesPerContext);
        return builder.build();
    }

    public static ApplicationContextRole createApplicationContextRole(AdminApplicationContext applicationContext) {
        List<String> roles = applicationContext.getRoles()
                                               .stream()
                                               .map(AdminApplicationContextRole::getRole)
                                               .collect(Collectors.toList());
        return ApplicationContextRole.newBuilder()
                                     .setContext(applicationContext.getContext())
                                     .addAllRoles(roles)
                                     .build();
    }


    public static AdminApplication createJpaApplication(Application application) {
        List<AdminApplicationContext> applicationContexts = application.getRolesPerContextList()
                                                                       .stream()
                                                                       .map(ApplicationProtoConverter::createJpaApplicationContext)
                                                                       .collect(Collectors.toList());

        return new AdminApplication(application.getName(),
                                    application.getDescription(),
                                    application.getTokenPrefix(),
                                    application.getToken(),
                                    applicationContexts,
                                    application.getMetaDataMap());
    }

    public static AdminApplicationContext createJpaApplicationContext(ApplicationContextRole applicationContextRole) {
        List<AdminApplicationContextRole> roles =
                applicationContextRole.getRolesList()
                                      .stream()
                                      .map(AdminApplicationContextRole::new)
                                      .collect(Collectors.toList());

        return new AdminApplicationContext(applicationContextRole.getContext(), roles);
    }
}
