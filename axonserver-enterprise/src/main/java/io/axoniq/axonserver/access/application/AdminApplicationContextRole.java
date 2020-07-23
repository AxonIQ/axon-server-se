package io.axoniq.axonserver.access.application;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * @author Milan Savic
 */
@Entity
@Table(name = "adm_application_binding_context_role")
public class AdminApplicationContextRole {

    @Id
    @GeneratedValue
    private Long id;

    private String role;

    @ManyToOne
    @JoinColumn(name = "application_context_id")
    private AdminApplicationContext applicationContext;

    public AdminApplicationContextRole() {
        // empty entity constructor
    }

    public AdminApplicationContextRole(String role) {
        this.role = role;
    }

    public Long getId() {
        return id;
    }

    public String getRole() {
        return role;
    }

    public AdminApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public void setApplicationContext(AdminApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
}
