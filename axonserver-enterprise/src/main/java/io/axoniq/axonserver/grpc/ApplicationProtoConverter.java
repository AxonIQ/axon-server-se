package io.axoniq.axonserver.grpc;


import io.axoniq.axonserver.grpc.internal.Action;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationRole;

import java.util.Date;

/**
 * Author: marc
 */
public class ApplicationProtoConverter {

    public static io.axoniq.axonserver.access.jpa.Application createJpaApplication(Application application) {
        io.axoniq.axonserver.access.jpa.ApplicationRole[] roles = new io.axoniq.axonserver.access.jpa.ApplicationRole[application.getRolesCount()];
        for( int i = 0; i < application.getRolesCount(); i++) {
            ApplicationRole role = application.getRoles(i);
            roles[i] = new io.axoniq.axonserver.access.jpa.ApplicationRole(role.getName(),
                    role.getContext(),
                    role.getEndDate() > 0 ? new Date(role.getEndDate()): null);
        }

        return new io.axoniq.axonserver.access.jpa.Application(application.getName(),application.getDescription(), application.getTokenPrefix(),
                application.getHashedToken(), roles);
    }

    public static Application createApplication(io.axoniq.axonserver.access.jpa.Application app, Action action) {
        Application.Builder builder = Application.newBuilder().setName(app.getName()).setAction(action);
        if( app.getDescription()!= null)
                builder.setDescription(app.getDescription());
        if( app.getHashedToken() != null)
                builder.setHashedToken(app.getHashedToken());
        if( app.getTokenPrefix() != null)
            builder.setTokenPrefix(app.getTokenPrefix());
        app.getRoles().forEach(role ->
                builder.addRoles(ApplicationRole.newBuilder()
                        .setName(role.getRole())
                        .setContext(role.getContext())
                        .setEndDate(role.getEndDate() != null? role.getEndDate().getTime(): 0)
                        .build())
        );
        return builder.build();
    }
}
