package io.axoniq.axonserver.access.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import static java.util.Arrays.asList;

/**
 * @author Marc Gathier
 */
@Entity
@Table(name = "adm_application_binding")
public class AdminApplication {

    @Id
    @GeneratedValue
    private Long id;

    @Column(unique = true)
    private String name;

    private String description;

    private String tokenPrefix;

    @Column(unique = true)
    private String hashedToken;

    @Column(name = "META_DATA")
    @Lob
    private String metaData;


    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER, mappedBy = "application")
    private Set<AdminApplicationContext> contexts = new HashSet<>();

    public AdminApplication() {
    }

    public AdminApplication(String name, String description, String tokenPrefix, String hashedToken,
                            AdminApplicationContext... contexts) {
        this(name, description, tokenPrefix, hashedToken, asList(contexts), Collections.emptyMap());
    }

    public AdminApplication(String name, String description, String tokenPrefix, String hashedToken,
                            List<AdminApplicationContext> contexts, Map<String, String> metaDataMap) {
        this.name = name;
        this.description = description;
        this.tokenPrefix = tokenPrefix;
        this.hashedToken = hashedToken;
        this.contexts.addAll(contexts);
        this.contexts.forEach(c -> c.setApplication(this));
        setMetaDataMap(metaDataMap);
    }


    public AdminApplication(String name) {
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

    public Set<AdminApplicationContext> getContexts() {
        return contexts;
    }

    public void setHashedToken(String hashedToken) {
        this.hashedToken = hashedToken;
    }

    public String getMetaData() {
        return metaData;
    }

    public void setMetaData(String metaData) {
        this.metaData = metaData;
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

    public void addContext(AdminApplicationContext applicationContext) {
        contexts.add(applicationContext);
        applicationContext.setApplication(this);
    }

    public void removeContext(String context) {
        for (Iterator<AdminApplicationContext> contextIterator = contexts.iterator(); contextIterator.hasNext(); ) {
            AdminApplicationContext applicationContext = contextIterator.next();
            if (applicationContext.getContext().equals(context)) {
                contextIterator.remove();
                applicationContext.setApplication(null);
            }
        }
    }

    public void addRole(String context, String role) {
        AdminApplicationContext applicationContext = contexts.stream().filter(c -> c.getContext().equals(context))
                                                             .findFirst().orElse(null);
        if( applicationContext == null) {
            applicationContext = new AdminApplicationContext(context,
                                                             Collections
                                                                     .singletonList(new AdminApplicationContextRole(role)));
            addContext(applicationContext);
        } else {
            applicationContext.addRole(new AdminApplicationContextRole(role));
        }
    }

    /**
     * Creates a copy (non-persisted) of the application with only the roles granted to the wildcard context ('*').
     *
     * @return copy of application with only wildcard roles
     */
    public AdminApplication newContextPermissions() {
        List<AdminApplicationContext> newContextPermissions = contexts.stream().filter(c -> c.getContext().equals("*"))
                                                                      .collect(Collectors.toList());
        return new AdminApplication(name,
                                    description,
                                    tokenPrefix,
                                    hashedToken,
                                    newContextPermissions,
                                    Collections.emptyMap());
    }

    public void setMetaDataMap(Map<String, String> metaDataMap) {
        this.metaData = null;
        try {
            this.metaData = new ObjectMapper().writeValueAsString(metaDataMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getMetaDataMap() {
        if (metaData == null) {
            return Collections.emptyMap();
        }

        try {
            return (Map<String, String>) new ObjectMapper().readValue(metaData, Map.class);
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyMap();
        }
    }
}
