package io.axoniq.axonserver.enterprise.config;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.util.YamlPropertySourceFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.logging.log4j.util.Strings.EMPTY;

/**
 * Cluster template POJO that reflects cluster-template YAML structure.
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Configuration
@PropertySource(
        value="file:${axoniq.axonserver.clustertemplate.path:./cluster-template.yml}",
        factory = YamlPropertySourceFactory.class,
        ignoreResourceNotFound = true)
@ConfigurationProperties(prefix="axoniq.axonserver.cluster-template")
public class ClusterTemplate {

    private static final String UNDEFINED = "UNDEFINED";

    private String first;
    private List<ReplicationsGroup> replicationGroups = new ArrayList<>();
    private List<Application> applications = new ArrayList<>();
    private List<User> users = new ArrayList<>();
    ;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public List<ReplicationsGroup> getReplicationGroups() {
        return replicationGroups;
    }

    public void setReplicationGroups(List<ReplicationsGroup> replicationGroups) {
        this.replicationGroups = replicationGroups;
    }

    public List<Application> getApplications() {
        return applications;
    }

    public void setApplications(List<Application> applications) {
        this.applications = applications;
    }

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }


    @KeepNames
    public static class ReplicationsGroup {

        private String name = UNDEFINED;
        private List<ReplicationGroupRole> roles = new ArrayList<>();
        private List<Context> contexts = new ArrayList<>();

        /**
         * No args constructor for use in serialization
         *
         */
        public ReplicationsGroup() {
        }

        public ReplicationsGroup(String name, List<ReplicationGroupRole> roles, List<Context> contexts) {
            super();
            this.name = name;
            this.roles = roles;
            this.contexts = contexts;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<ReplicationGroupRole> getRoles() {
            return roles;
        }

        public void setRoles(List<ReplicationGroupRole> roles) {
            this.roles = roles;
        }

        public List<Context> getContexts() {
            return contexts;
        }

        public void setContexts(List<Context> contexts) {
            this.contexts = contexts;
        }
    }

    @KeepNames
    public static class Context {

        private String name = UNDEFINED;

        private Map<String,String> metaData = Collections.emptyMap();

        /**
         * No args constructor for use in serialization
         *
         */
        public Context() {
        }

        public Context(String name) {
            this.name = name;
        }

        public Context(String name, Map<String, String> metaData) {
            this.name = name;
            this.metaData = metaData;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, String> getMetaData() {
            return metaData;
        }

        public void setMetaData(Map<String, String> metaData) {
            this.metaData = metaData;
        }
    }

    @KeepNames
    public static class ReplicationGroupRole {

        private String node = UNDEFINED;
        private String role = UNDEFINED;

        /**
         * No args constructor for use in serialization
         */
        public ReplicationGroupRole() {
        }

        /**
         *
         * @param node
         * @param role
         */
        public ReplicationGroupRole(String node, String role) {
            super();
            this.node = node;
            this.role = role;
        }

        public String getNode() {
            return node;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }
    }

    @KeepNames
    public static class Application {

        private String name = UNDEFINED;
        private String description = EMPTY;
        private List<ApplicationRole> roles = new ArrayList<>();
        private String token = UNDEFINED;

        /**
         * No args constructor for use in serialization
         *
         */
        public Application() {
        }

        public Application(String name, String description, List<ApplicationRole> roles, String token) {
            super();
            this.name = name;
            this.description = description;
            this.roles = roles;
            this.token = token;
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

        public List<ApplicationRole> getRoles() {
            return roles;
        }

        public void setRoles(List<ApplicationRole> roles) {
            this.roles = roles;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

    }

    @KeepNames
    public static class ApplicationRole {

        private String context = UNDEFINED;
        private List<String> roles = new ArrayList<>();

        /**
         * No args constructor for use in serialization
         *
         */
        public ApplicationRole() {
        }

        public ApplicationRole(String context, List<String> roles) {
            super();
            this.context = context;
            this.roles = roles;
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

    }

    @KeepNames
    public static class User {

        private String userName = UNDEFINED;
        private List<UserRole> roles = new ArrayList<>();
        private String password = UNDEFINED;
        /**
         * No args constructor for use in serialization
         *
         */
        public User() {
        }

        public User(String userName, List<UserRole> roles, String password) {
            super();
            this.userName = userName;
            this.roles = roles;
            this.password = password;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public List<UserRole> getRoles() {
            return roles;
        }

        public void setRoles(List<UserRole> roles) {
            this.roles = roles;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

    }

    @KeepNames
    public static class UserRole {

        private String context = UNDEFINED;
        private List<String> roles = new ArrayList<>();

        /**
         * No args constructor for use in serialization
         *
         */
        public UserRole() {
        }

        public UserRole(String context, List<String> roles) {
            super();
            this.context = context;
            this.roles = roles;
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

    }

}
