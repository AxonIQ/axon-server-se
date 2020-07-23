package io.axoniq.axonserver.access.application;


import java.util.List;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * Created by marc on 7/13/2017.
 */
@Entity
@Table(name = "adm_application_binding_context")
public class AdminApplicationContext {

    @Id
    @GeneratedValue
    private Long id;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER, mappedBy = "applicationContext")
    private List<AdminApplicationContextRole> roles;

    @ManyToOne
    @JoinColumn(name = "application_id")
    private AdminApplication application;

    private String context;

    public AdminApplicationContext() {
    }

    public AdminApplicationContext(String context, List<AdminApplicationContextRole> roles) {
        this.roles = roles;
        roles.forEach(r -> r.setApplicationContext(this));
        this.context = context;
    }

    public List<AdminApplicationContextRole> getRoles() {
        return roles;
    }

    public String getContext() {
        return context;
    }

    public boolean hasRole(String role) {
        return roles.stream()
                    .map(AdminApplicationContextRole::getRole)
                    .anyMatch(role::equals);
    }

    public AdminApplication getApplication() {
        return application;
    }

    public void setApplication(AdminApplication application) {
        this.application = application;
    }

    public void addRole(AdminApplicationContextRole applicationContextRole) {
        applicationContextRole.setApplicationContext(this);
        roles.add(applicationContextRole);
    }
}
