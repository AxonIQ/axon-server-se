package io.axoniq.platform.user;

import javax.persistence.*;

/**
 * Author: marc
 */
@Entity
@Table(name="user_roles")
public class UserRole {
    @Id
    @Column(name="user_role_id")
    @GeneratedValue
    private Long id;

    private String role;

    @ManyToOne
    @JoinColumn(name = "username")
    private User user;


    public UserRole(User user, String role) {
        this.user = user;
        this.role = role;
    }

    public UserRole() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }
}
