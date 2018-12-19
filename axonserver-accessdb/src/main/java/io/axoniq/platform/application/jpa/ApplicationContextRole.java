package io.axoniq.platform.application.jpa;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

/**
 * @author Milan Savic
 */
@Entity
public class ApplicationContextRole {

    @Id
    @GeneratedValue
    private Long id;

    private String role;

    @ManyToOne
    @JoinColumn(name="application_context_id")
    private ApplicationContext applicationContext;

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

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
}
