package io.axoniq.platform.application.jpa;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * @author Milan Savic
 */
@Entity
public class ApplicationContextRole {

    @Id
    @GeneratedValue
    private Long id;

    private String role;

    public ApplicationContextRole() {
        // empty entity constructor
    }

    public ApplicationContextRole(String role) {
        this.role = role;
    }

    public Long getId() {
        return id;
    }

    public String getRole() {
        return role;
    }
}
