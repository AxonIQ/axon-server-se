/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.user;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;

/**
 * Access to stored users. Defined users have access to the web services/pages.
 *
 * @author Marc Gathier
 */
@Controller
public class UserController {
    private final PasswordEncoder passwordEncoder;
    private final UserRepository userRepository;


    public UserController(PasswordEncoder passwordEncoder, UserRepository userRepository) {
        this.passwordEncoder = passwordEncoder;
        this.userRepository = userRepository;
    }

    @Transactional
    public void deleteUser(String username) {
        synchronized (userRepository) {
            userRepository.findById(username).ifPresent(u -> {
                userRepository.delete(u);
                userRepository.flush();
            });
        }
    }

    public List<User> getUsers() {
        return userRepository.findAll();
    }

    private User syncUser(String username, String password, Set<UserRole> roles) {
        synchronized (userRepository) {
            if (StringUtils.isEmpty(password)) {
                password = getPassword(username);
            }
            User user = userRepository.save(new User(username, password, roles));
            userRepository.flush();
            return user;
        }
    }

    /**
     * Updates/creates a user with specified password and roles. If password is set, it will be hashed. If no password
     * is set and
     * user already exists the stored password is not changed.
     *
     * @param username the username
     * @param password the plaintext password
     * @param roles    the roles granted to the user
     * @return the stored information, including the hashed password
     */
    @Transactional
    public User updateUser(String username, String password, Set<UserRole> roles) {
        return syncUser(username, password == null ? null: passwordEncoder.encode(password), roles);
    }

    @Transactional
    public void syncUser(User jpaUser) {
        synchronized (userRepository) {
            if (StringUtils.isEmpty(jpaUser.getPassword())) {
                jpaUser.setPassword(getPassword(jpaUser.getUserName()));
            }
            userRepository.save(jpaUser);
        }
    }

    public String getPassword(String userName) {
        return userRepository.findById(userName).map(User::getPassword).orElse(null);
    }

    public User findUser(String name) {
        return userRepository.getOne(name);
    }

    /**
     * Removes all roles for specified context from all users.
     *
     * @param context the context to remove
     */
    @Transactional
    public void removeRolesForContext(String context) {
        userRepository.findAll().forEach(user -> user.removeContext(context));
    }

    @Transactional
    public void deleteAll() {
        userRepository.deleteAll();
    }
}
