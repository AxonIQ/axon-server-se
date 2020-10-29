/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.extensions.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.test.TestUtils;
import org.junit.*;

/**
 * @author Marc Gathier
 */
public class OsgiControllerTest {

    @Test
    public void start() {
        OsgiController osgiController = new OsgiController(TestUtils.fixPathOnWindows(OsgiController
                                                                                              .class.getResource(
                "/sample-bundles")
                                                                                                    .getFile()),
                                                           "4.5.0");
        osgiController.start();

        osgiController.getServices(AppendEventInterceptor.class).forEach(s -> System.out.println(s));
        osgiController.listBundles().forEach(s -> System.out.println(s));
    }
}