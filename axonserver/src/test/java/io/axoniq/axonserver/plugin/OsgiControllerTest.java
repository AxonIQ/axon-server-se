/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.plugin.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.test.TestUtils;
import org.junit.*;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class OsgiControllerTest {

    private final OsgiController testSubject = new OsgiController("cache",
                                                                  "onFirstInit",
                                                                  true,
                                                                  HashMap::new);

    @Before
    public void setup() {
        testSubject.start();
    }

    @Test
    public void loadBundle() throws IOException, BundleException, RequestRejectedException {
        File bundlesDir = new File(TestUtils.fixPathOnWindows(PluginController
                                                                      .class.getResource(
                "/sample-bundles").getFile()));
        File[] files = bundlesDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (files != null) {
            for (File file : files) {
                testSubject.addPlugin(file);
            }
        }

        assertEquals(1, testSubject.listPlugins().size());

        Set<ServiceWithInfo<CommandRequestInterceptor>> interceptors = testSubject.getServicesWithInfo(
                CommandRequestInterceptor.class);
        assertEquals(1, interceptors.size());
    }

    @Test
    public void getAxonServerProperties() {
        Optional<AxonServerInformationProvider> axonServerContextProvider = testSubject.get(
                AxonServerInformationProvider.class);
        assertTrue(axonServerContextProvider.isPresent());

        AxonServerInformationProvider service = axonServerContextProvider.get();
        System.out.println(service.properties());
    }
}