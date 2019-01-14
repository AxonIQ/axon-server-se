package io.axoniq.platform.application.jpa;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

/**
 * @author Marc Gathier
 */
@Entity
public class Application {

    @Id
    @GeneratedValue
    private Long id;

    @Column(unique = true)
    private String name;

    private String description;

    private String tokenPrefix;

    @Column(unique = true)
    private String hashedToken;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    private Set<ApplicationRole> roles = new HashSet<>();

    public Application() {
    }

    public Application(String name, String description, String tokenPrefix, String hashedToken, ApplicationRole... roles) {
        this.name = name;
        this.description = description;
        this.tokenPrefix = tokenPrefix;
        this.hashedToken = hashedToken;
        this.roles.addAll(Arrays.asList(roles));
    }

    public Application(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getTokenPrefix() {
        return tokenPrefix;
    }

    public Set<ApplicationRole> getRoles() {
        return roles;
    }

    public void setHashedToken(String hashedToken) {
        this.hashedToken = hashedToken;
    }

    public boolean hasRoleForContext(String requiredRole, String context) {
        Date now = new Date();
        return roles.stream()
                .anyMatch(role -> context.equals(role.getContext()) && role.getRole().equals(requiredRole) && (role.getEndDate() == null || role.getEndDate().after(now)));

    }

    public String getHashedToken() {
        return hashedToken;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setTokenPrefix(String tokenPrefix) {
        this.tokenPrefix = tokenPrefix;
    }
}
