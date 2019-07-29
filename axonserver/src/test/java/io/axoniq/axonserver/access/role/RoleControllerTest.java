/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.role;

import io.axoniq.axonserver.AxonServer;
import io.axoniq.axonserver.access.roles.RoleController;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by Sara Pellegrini on 08/03/2018.
 * sara.pellegrini@gmail.com
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@ComponentScan(basePackages = "io.axoniq.axonserver.access", lazyInit = true)
@ContextConfiguration(classes = AxonServer.class)
public class RoleControllerTest {

    private RoleController roleController;

//    @Autowired
//    private RoleRepository roleRepository;

    @Before
    public void before(){
        // roleController = new RoleController(roleRepository);
    }

    @Test
    public void testFindUserRoles(){
//        List<Role> roles = roleController.listUserRoles();
//        assertThat(roles.size(), equalTo(2));
//        List<String> roleNames = roles.stream().map(Role::name).sorted().collect(toList());
//        assertThat(roleNames, contains("ADMIN","READ" ));
    }


    @Test
    public void testFindApplicationRoles(){
//        List<Role> roles = roleController.listApplicationRoles();
//        assertThat(roles.size(), equalTo(3));
//        List<String> roleNames = roles.stream().map(Role::name).sorted().collect(toList());
//        assertThat(roleNames, contains("ADMIN", "READ","WRITE" ));
    }



}
