package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.access.application.ApplicationContext;
import io.axoniq.axonserver.access.application.ApplicationContextRole;
import io.axoniq.axonserver.access.application.JpaApplication;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@KeepNames
public class ApplicationJSON {

    private String name;

    private String description;

    private String token;
    private List<ApplicationRoleJSON> roles = new ArrayList<>();

    public ApplicationJSON() {
    }

    public ApplicationJSON(JpaApplication application) {
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



        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        public List<String> getRoles() {
            return roles;
        }

        public void setRoles(List<String> roles) {
            this.roles = roles;
        }

        public ApplicationContext toApplicationRole() {
            return new ApplicationContext(context, roles.stream()
                                                        .map(ApplicationContextRole::new)
                                                        .collect(Collectors.toList()));
        }
    }
}
