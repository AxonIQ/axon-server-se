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
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

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

    private User syncUser(String username, String password, String[] roles) {
        synchronized (userRepository) {
            if (password == null) {
                password = userRepository.findById(username).map(User::getPassword).orElse(null);
            }
            User user = userRepository.save(new User(username, password, roles));
            userRepository.flush();
            return user;
        }
    }

    @Transactional
    public User updateUser(String username, String password, String[] roles) {
        return syncUser(username, password == null ? null: passwordEncoder.encode(password), roles);
    }

    @Transactional
    public void syncUser(User jpaUser) {
        synchronized (userRepository) {
            userRepository.save(jpaUser);
        }
    }
}
