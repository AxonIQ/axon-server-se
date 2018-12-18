package io.axoniq.platform.application.jpa;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

import static java.util.Arrays.asList;

/**
 * Created by marc on 7/13/2017.
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

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER, mappedBy = "application")
    private Set<ApplicationContext> contexts = new HashSet<>();

    public Application() {
    }

    public Application(String name, String description, String tokenPrefix, String hashedToken,
                       ApplicationContext... contexts) {
        this(name, description, tokenPrefix, hashedToken, asList(contexts));
    }

    public Application(String name, String description, String tokenPrefix, String hashedToken,
                       List<ApplicationContext> contexts) {
        this.name = name;
        this.description = description;
        this.tokenPrefix = tokenPrefix;
        this.hashedToken = hashedToken;
        this.contexts.addAll(contexts);
        this.contexts.forEach(c -> c.setApplication(this));
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

    public Set<ApplicationContext> getContexts() {
        return contexts;
    }

    public void setHashedToken(String hashedToken) {
        this.hashedToken = hashedToken;
    }

    public boolean hasRoleForContext(String requiredRole, String context) {
        return contexts.stream()
                       .anyMatch(applicationContext -> context.equals(applicationContext.getContext())
                               && applicationContext.hasRole(requiredRole));
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

    public void addContext(ApplicationContext applicationContext) {
        contexts.add(applicationContext);
        applicationContext.setApplication(this);
    }

    public void removeContext(String context) {
        for(Iterator<ApplicationContext> contextIterator = contexts.iterator(); contextIterator.hasNext(); ) {
            ApplicationContext applicationContext = contextIterator.next();
            if( applicationContext.getContext().equals(context)) {
                contextIterator.remove();
                applicationContext.setApplication(null);
            }
        }
    }
}
