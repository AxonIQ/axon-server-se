package io.axoniq.platform.application.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * @author Marc Gathier
 */
@Entity
public class PathMapping {
    @Id
    private String path;

    private String role;

    public PathMapping() {
    }

    public PathMapping(String path, String role) {
        this.path = path;
        this.role = role;
    }

    public String getPath() {
        return path;
    }

    public String getRole() {
        return role;
    }
}
