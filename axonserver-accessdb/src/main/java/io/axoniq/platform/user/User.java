package io.axoniq.platform.user;

import javax.persistence.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: marc
 */
@Entity
@Table(name="users")
public class User {
    @Id
    @Column(name="username")
    private String userName;
    private String password;
    private boolean enabled;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER, mappedBy = "user")
    private Set<UserRole> roles = new HashSet<>();

    public User(String userName, String password) {
        this(userName, password, new String[]{"USER"});
    }

    public User(String userName, String password, String[] roles) {
        this.userName = userName;
        this.password = password;
        this.enabled = true;
        Arrays.stream(roles).forEach(r -> this.roles.add(new UserRole(this, r)));
    }

    public User() {
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Set<UserRole> getRoles() {
        return roles;
    }

    public void setRoles(Set<UserRole> roles) {
        this.roles = roles;
    }
}
