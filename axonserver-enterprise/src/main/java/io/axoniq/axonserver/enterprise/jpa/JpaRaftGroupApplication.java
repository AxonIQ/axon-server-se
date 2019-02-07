package io.axoniq.axonserver.enterprise.jpa;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * Author: marc
 */
@Entity
public class JpaRaftGroupApplication {

    @GeneratedValue
    @Id
    private Long id;

    private String groupId;

    private String name;

    private String tokenPrefix;

    private String hashedToken;

    @ElementCollection
    private Set<String> roles = new HashSet<>();

    JpaRaftGroupApplication() {

    }
    public JpaRaftGroupApplication(String groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
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

    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }
}
