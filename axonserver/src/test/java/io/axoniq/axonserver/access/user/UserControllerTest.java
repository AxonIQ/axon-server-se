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
import org.junit.*;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class UserControllerTest {

    private UserController testSubject;
    private List<User> users;

    @Before
    public void setup() {
        UserRepository userRepository = mock(UserRepository.class);

        users = new ArrayList<>();
        users.add(new User("Demo", "Encoded:Demo"));
        when(userRepository.findAll()).thenReturn(users);
        doAnswer(invocationOnMock -> {
            String id = (String)invocationOnMock.getArguments()[0];
            return users.stream().filter(u -> u.getUserName().equals(id)).findFirst();
        }).when(userRepository).findById(any(String.class));
        doAnswer(invocationOnMock -> {
            User user = (User)invocationOnMock.getArguments()[0];
            users.remove(user);
            users.add(user);
            return user;
        }).when(userRepository).save(any(User.class));
        doAnswer(invocationOnMock -> {
            User user = (User)invocationOnMock.getArguments()[0];
            users.remove(user);
            return null;
        }).when(userRepository).delete(any(User.class));

        testSubject = new UserController(new PasswordEncoder() {
            @Override
            public String encode(CharSequence charSequence) {
                return "Encoded:" + charSequence;
            }

            @Override
            public boolean matches(CharSequence charSequence, String s) {
                return false;
            }
        }, userRepository);
    }

    @Test
    public void deleteUser() {
        testSubject.deleteUser("Demo");
        assertTrue(users.isEmpty());
    }

    @Test
    public void getUsers() {
        assertEquals(1, testSubject.getUsers().size());
    }

    @Test
    public void updateUserWithPassword() {
        assertEquals("Encoded:newpassword", testSubject.updateUser("Demo", "newpassword", new String[0]).getPassword());
    }

    @Test
    public void updateUserWithoutPassword() {
        assertEquals("Encoded:Demo", testSubject.updateUser("Demo", null, new String[0]).getPassword());
    }

    @Test
    public void newUser() {
        assertEquals("Encoded:newpassword", testSubject.updateUser("Demo2", "newpassword", new String[0]).getPassword());
    }

    @Test
    public void syncUser() {
        testSubject.syncUser(new User("Demo", "TEST"));
        assertEquals("TEST", users.get(0).getPassword());
    }
}