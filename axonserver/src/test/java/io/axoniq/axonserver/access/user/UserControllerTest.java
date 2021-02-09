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
import java.util.Collections;
import java.util.List;

import static io.axoniq.axonserver.access.user.UserController.PWD_NOLOGON;
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
    private PasswordEncoder passwordEncoder;

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

        passwordEncoder = new PasswordEncoder() {
            @Override
            public String encode(CharSequence charSequence) {
                return "Encoded:" + charSequence;
            }

            @Override
            public boolean matches(CharSequence charSequence, String s) {
                return false;
            }
        };
        testSubject = new UserController(passwordEncoder, userRepository);
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
        assertEquals("Encoded:newpassword",
                     testSubject.updateUser("Demo", "newpassword", Collections.emptySet()).getPassword());
    }

    @Test
    public void updateUserWithoutPassword() {
        assertEquals("Encoded:Demo", testSubject.updateUser("Demo", null, Collections.emptySet()).getPassword());
    }

    @Test
    public void newUser() {
        assertEquals("Encoded:newpassword",
                     testSubject.updateUser("Demo2", "newpassword", Collections.emptySet()).getPassword());
    }

    @Test
    public void newUserWithoutPassword() {
        assertEquals(PWD_NOLOGON,
                     testSubject.updateUser("Demo3", null, Collections.emptySet()).getPassword());
    }

    @Test
    public void syncUser() {
        testSubject.syncUser(new User("Demo", "newpassword"));
        assertEquals("newpassword", users.get(0).getPassword());
    }

    @Test
    public void syncUserWithoutPassword() {
        testSubject.updateUser("Demo4", "newpassword", Collections.emptySet());
        User jpaUser = testSubject.findUser("Demo4");
        jpaUser = new User(jpaUser.getUserName(), null, jpaUser.getRoles());
        testSubject.syncUser(jpaUser);
        assertEquals(passwordEncoder.encode("newpassword"), testSubject.findUser("Demo4").getPassword());
    }

    @Test
    public void syncNewUser() {
        testSubject.syncUser(new User("Demo5", passwordEncoder.encode("newpassword"), Collections.emptySet()));
        assertEquals("Encoded:newpassword", testSubject.findUser("Demo5").getPassword());
    }

    @Test
    public void syncNewUserWithoutPassword() {
        testSubject.syncUser(new User("Demo6", null, Collections.emptySet()));
        assertEquals(PWD_NOLOGON, testSubject.findUser("Demo6").getPassword());
    }
}