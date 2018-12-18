package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.platform.application.jpa.Application;
import io.axoniq.platform.application.jpa.ApplicationContext;
import io.axoniq.platform.application.jpa.ApplicationContextRole;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.axoniq.platform.util.StringUtils.getOrDefault;

/**
 * Author: marc
 */
@KeepNames
public class ApplicationJSON {

    private String name;

    private String description;

    private String token;
    private List<ApplicationRoleJSON> roles = new ArrayList<>();

    public ApplicationJSON() {
    }

    public ApplicationJSON(Application application) {
        name = application.getName();
        description = application.getDescription();
        roles = application.getContexts().stream().map(ApplicationRoleJSON::new).collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public List<ApplicationRoleJSON> getRoles() {
        return roles;
    }

    public void setRoles(List<ApplicationRoleJSON> roles) {
        this.roles = roles;
    }

    public Application toApplication() {
        return new Application(name, description, null, token,
                               roles.stream().map(ApplicationRoleJSON::toApplicationRole)
                                    .toArray(ApplicationContext[]::new));
    }

    public io.axoniq.axonserver.grpc.internal.Application asProto() {
        return io.axoniq.axonserver.grpc.internal.Application.newBuilder()
                                                             .setName(name)
                                                             .setDescription(getOrDefault(description, ""))
                                                             .setToken(getOrDefault(token, UUID.randomUUID().toString()))
                                                             .addAllRolesPerContext(
                                                                     roles.stream().map(ApplicationRoleJSON::asApplicationContextRole).collect(
                                                                             Collectors.toList())
                                                             )
                .build();
    }

    @KeepNames
    public static class ApplicationRoleJSON {

        private List<String> roles;

        private String context;

        public ApplicationRoleJSON() {

        }

        public ApplicationRoleJSON(ApplicationContext applicationRole) {
            roles = applicationRole.getRoles()
                                   .stream()
                                   .map(ApplicationContextRole::getRole)
                                   .collect(Collectors.toList());
            context = applicationRole.getContext();
        }

        public List<String> getRoles() {
            return roles;
        }

        public void setRoles(List<String> role) {
            this.roles = role;
        }

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        public ApplicationContext toApplicationRole() {
            return new ApplicationContext(context, roles.stream()
                                                        .map(ApplicationContextRole::new)
                                                        .collect(Collectors.toList()));
        }

        public io.axoniq.axonserver.grpc.internal.ApplicationContextRole asApplicationContextRole() {
            return io.axoniq.axonserver.grpc.internal.ApplicationContextRole.newBuilder()
                                                                            .setContext(context)
                                                                            .addAllRoles(roles)
                                                                            .build();
        }
    }
}
