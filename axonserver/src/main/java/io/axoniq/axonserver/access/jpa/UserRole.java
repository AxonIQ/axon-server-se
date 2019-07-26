/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.jpa;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * A user/role combination.
 *
 * @author Marc Gathier
 */
@Entity
@Table(name="user_roles")
public class UserRole {
    @Id
    @Column(name="user_role_id")
    @GeneratedValue
    private Long id;

    private String role;

    private String context;

    @ManyToOne
    @JoinColumn(name = "username")
    private User user;


    public UserRole(User user, String role, String context) {
        this.user = user;
        this.role = role;
        this.context = context;
    }

    public UserRole() {
    }

    public UserRole(String context, String role) {
        this.role = role;
        this.context = context;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return role + "@" + context;
    }

    public static UserRole parse(String s) {
        String[] parts = s.split("@", 2);
        UserRole userRole = new UserRole();
        userRole.setRole(parts[0]);
        if (parts.length > 1) {
            userRole.setContext(parts[1]);
        } else {
            userRole.setContext("*");
        }
        return userRole;
    }
}
