package io.axoniq.axonserver.access.user;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CollectionTable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * User information per context.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Entity
@Table(name = "rg_user_context")
public class ReplicationGroupUser {

    @GeneratedValue
    @Id
    private Long id;

    private String context;

    private String username;

    @JsonIgnore
    private String password;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "rg_user_context_role")
    private Set<String> roles = new HashSet<>();

    public ReplicationGroupUser() {

    }

    public ReplicationGroupUser(String context, String username) {
        this.context = context;
        this.username = username;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public Set<String> getQualifiedRoles() {
        return roles.stream().map(role -> role + '@' + context).collect(Collectors.toSet());
    }

    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }
}
