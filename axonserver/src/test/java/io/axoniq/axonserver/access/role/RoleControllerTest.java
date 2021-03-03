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
import io.axoniq.axonserver.access.jpa.Role;
import io.axoniq.axonserver.access.roles.RoleController;
import io.axoniq.axonserver.access.roles.RoleRepository;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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

    @Autowired
    private RoleRepository roleRepository;

    @MockBean
    private VersionInfoProvider versionInfoProvider;

    @Before
    public void before(){
        roleController = new RoleController(roleRepository);
    }

    @Test
    public void testFindRoles() {
        List<Role> roles = roleController.listRoles();
        assertThat(roles.size(), equalTo(13));
        List<String> roleNames = roles.stream().map(Role::getRole).sorted().collect(toList());
        assertThat(roleNames, contains("ADMIN", "CONTEXT_ADMIN", "DISPATCH_COMMANDS", "DISPATCH_QUERY",
                                       "MONITOR", "PUBLISH_EVENTS", "READ", "READ_EVENTS", "SUBSCRIBE_COMMAND_HANDLER",
                                       "SUBSCRIBE_QUERY_HANDLER", "USE_CONTEXT", "VIEW_CONFIGURATION", "WRITE"));
    }
}
