/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.jpa;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * Defines users to access the Axon Dashboard and their roles.
 *
 * @author Marc Gathier
 */
@Entity
@Table(name="users")
public class User {
    @Id
    @Column(name="username")
    private String userName;
    private String password;
    private boolean enabled;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @JoinColumn(name = "username")
    private Set<UserRole> roles = new HashSet<>();

    public User(String userName, String password) {
        this(userName, password, Collections.emptySet());
    }

    public User(String userName, String password, Set<UserRole> userRoles) {
        this.userName = userName;
        this.password = password;
        this.enabled = true;
        if (userRoles != null) {
            userRoles.forEach(r -> roles.add(new UserRole(r.getContext(), r.getRole())));

        }
    }

    public User() {
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Set<UserRole> getRoles() {
        return roles;
    }

    public void setRoles(Set<UserRole> roles) {
        this.roles = roles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        User user = (User) o;
        return Objects.equals(userName, user.userName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName);
    }

    /**
     * Creates a copy of the user (non-persisted) that contains all roles assigned to the wildcard context.
     *
     * @param user
     * @return
     */
    public static User newContextPermissions(User user) {
        User newUser = new User(user.userName, user.password);
        newUser.setRoles(user.getRoles()
                             .stream()
                             .filter(userRole -> "*".equals(userRole.getContext()))
                             .collect(Collectors.toSet()));

        return newUser;
    }

    /**
     * Remobes all roles from the user for specified {@code context}
     *
     * @param context the context to remove
     */
    public void removeContext(String context) {
        roles.removeIf(r -> r.getContext().equals(context));
    }

    @Override
    public String toString() {
        return "User{" +
                "userName='" + userName + '\'' +
                ", password='PROTECTED'" +
                ", enabled=" + enabled +
                ", roles=" + roles +
                '}';
    }
}
