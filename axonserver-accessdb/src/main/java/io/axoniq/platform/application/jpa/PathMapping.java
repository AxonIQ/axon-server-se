package io.axoniq.platform.application.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Created by marc on 7/17/2017.
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
