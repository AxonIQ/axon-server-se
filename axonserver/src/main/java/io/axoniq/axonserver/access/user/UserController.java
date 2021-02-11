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
    /**
     * An account without a password gets this value  instead.
     */
    public static final String PWD_NOLOGON = "nologon";

    private final PasswordEncoder passwordEncoder;
    private final UserRepository userRepository;


    public UserController(PasswordEncoder passwordEncoder, UserRepository userRepository) {
        this.passwordEncoder = passwordEncoder;
        this.userRepository = userRepository;
    }

    /**
     * Remove a user by name, if currently defined in the database.
     *
     * @param username the name of the user.
     */
    @Transactional
    public void deleteUser(String username) {
        synchronized (userRepository) {
            userRepository.findById(username).ifPresent(u -> {
                userRepository.delete(u);
                userRepository.flush();
            });
        }
    }

    /**
     * Return a list of all users.
     *
     * @return a {@link List} of all {@link User}s.
     */
    public List<User> getUsers() {
        return userRepository.findAll();
    }

    /**
     * Updates/creates a user with specified password and roles. If password is set, it will be hashed. If no password
     * is set and the user already exists the stored password is not changed. If the user does not exist, and the
     * password is {@code null}, the user will get a {@link #PWD_NOLOGON} marker instead.
     *
     * Note that the {@link #PWD_NOLOGON} marker prevents a match with any password, as it is not hashed.
     *
     * @param username the username
     * @param password the plaintext password
     * @param roles    the roles granted to the user
     * @return the stored information, including the current (hashed) password.
     */
    @Transactional
    public User updateUser(String username, String password, Set<UserRole> roles) {
        synchronized (userRepository) {
            if (StringUtils.isEmpty(password)) {
                final User currentUser = findUser(username);

                password = (currentUser != null) ? currentUser.getPassword() : PWD_NOLOGON;
            } else {
                password = passwordEncoder.encode(password);
            }
            User user = userRepository.save(new User(username, password, roles));
            userRepository.flush();
            return user;
        }
    }

    /**
     * Updates/creates a {@link User} in the database. If a password is set, it will be hashed. If no password
     * is set and the user already exists the stored password is not changed. If the user does not exist, and the
     * password is not set, the user will get a {@link #PWD_NOLOGON} marker instead.
     *
     * Note that the {@link #PWD_NOLOGON} marker prevents a match with any password, as it is not hashed.
     *
     * @param jpaUser the {@link User} object to store.
     */
    @Transactional
    public void syncUser(User jpaUser) {
        synchronized (userRepository) {
            if (StringUtils.isEmpty(jpaUser.getPassword())) {
                final User currentUser = findUser(jpaUser.getUserName());
                jpaUser.setPassword((currentUser == null) ? PWD_NOLOGON : currentUser.getPassword());
            }
            userRepository.save(jpaUser);
        }
    }

    /**
     * Return the current (hashed) password of a user with the given name, or {@code null} if the user is not found.
     *
     * @param userName the name of the user to search for.
     * @return the current (hashed) password of a user, or {@code null} if the user is not found.
     */
    public String getPassword(String userName) {
        return userRepository.findById(userName).map(User::getPassword).orElse(null);
    }

    /**
     * Return the {@link User} with the given name if found, or {@code null} if not found.
     *
     * @param userName the name of the user to search for.
     * @return the {@link User} if found, or {@code null} if not found.
     */
    public User findUser(String userName) {
        return userRepository.findById(userName).orElse(null);
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

    /**
     * Remove all users in the repository.
     */
    @Transactional
    public void deleteAll() {
        userRepository.deleteAll();
    }
}
