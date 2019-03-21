package io.axoniq.axonserver.access.jpa;

import io.axoniq.axonserver.KeepNames;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;

/**
 * Holds defined roles. Roles may be defined on an application or user level.
 *
 * @author Sara Pellegrini
 */
@Entity
public class Role {

    @Id
    private String name;

    @ElementCollection
    @Enumerated(EnumType.STRING)
    private Set<Type> types = new HashSet<>();


    @KeepNames
    public enum Type {
        USER,APPLICATION
    }

    public Role() {
    }

    public Role(String name, Type ... types) {
        this.name = name;
        this.types.addAll(Arrays.asList(types));
    }

    public String name() {
        return name;
    }
}
