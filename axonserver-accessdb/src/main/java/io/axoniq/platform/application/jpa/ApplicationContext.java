package io.axoniq.platform.application.jpa;


import java.util.List;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

/**
 * Created by marc on 7/13/2017.
 */
@Entity
public class ApplicationContext {

    @Id
    @GeneratedValue
    private Long id;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    private List<ApplicationContextRole> roles;

    private String context;

    public ApplicationContext() {
    }

    public ApplicationContext(String context, List<ApplicationContextRole> roles) {
        this.roles = roles;
        this.context = context;
    }

    public List<ApplicationContextRole> getRoles() {
        return roles;
    }

    public String getContext() {
        return context;
    }

    public boolean hasRole(String role) {
        return roles.stream()
                    .map(ApplicationContextRole::getRole)
                    .anyMatch(role::equals);
    }
}
