package io.axoniq.axonserver.access.application;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * @author Marc Gathier
 */
@Entity
public class JpaContextApplication {

    @GeneratedValue
    @Id
    private Long id;

    private String context;

    private String name;

    private String tokenPrefix;

    private String hashedToken;

    @ElementCollection(fetch = FetchType.EAGER)
    private Set<String> roles = new HashSet<>();

    JpaContextApplication() {

    }
    public JpaContextApplication(String context, String name) {
        this.context = context;
        this.name = name;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTokenPrefix() {
        return tokenPrefix;
    }

    public void setTokenPrefix(String tokenPrefix) {
        this.tokenPrefix = tokenPrefix;
    }

    public String getHashedToken() {
        return hashedToken;
    }

    public void setHashedToken(String hashedToken) {
        this.hashedToken = hashedToken;
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
