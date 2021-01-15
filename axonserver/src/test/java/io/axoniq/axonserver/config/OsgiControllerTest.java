/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.extensions.ExtensionController;
import io.axoniq.axonserver.extensions.ExtensionKey;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.OsgiController;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.interceptor.DefaultInterceptorContext;
import io.axoniq.axonserver.test.TestUtils;
import org.junit.*;
import org.osgi.framework.BundleException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author Marc Gathier
 */
public class OsgiControllerTest {

    @Test
    public void start() throws IOException, BundleException {
        OsgiController osgiController = new OsgiController(TestUtils.fixPathOnWindows(ExtensionController
                                                                                              .class.getResource(
                "/sample-bundles")
                                                                                                    .getFile()),
                                                           "cache",
                                                           "onFirstInit");
        osgiController.start();

        Command command = Command.newBuilder().setClientId("sample").build();
        List<ServiceWithInfo<CommandRequestInterceptor>> interceptors =
                StreamSupport.stream(osgiController.getServicesWithInfo(CommandRequestInterceptor.class).spliterator(),
                                     false)
                             .sorted(Comparator.comparing(ServiceWithInfo::order))
                             .collect(Collectors.toList());

        ExtensionUnitOfWork unitOfWork = new DefaultInterceptorContext("context", new Authentication() {
            @Override
            public Collection<? extends GrantedAuthority> getAuthorities() {
                return null;
            }

            @Override
            public Object getCredentials() {
                return null;
            }

            @Override
            public Object getDetails() {
                return null;
            }

            @Override
            public Object getPrincipal() {
                return "principal";
            }

            @Override
            public boolean isAuthenticated() {
                return true;
            }

            @Override
            public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {

            }

            @Override
            public String getName() {
                return "name";
            }
        });
        for (ServiceWithInfo<CommandRequestInterceptor> interceptor : interceptors) {
            command = interceptor.service().commandRequest(command, unitOfWork);
        }
        System.out.println(command);

        osgiController.listExtensions().forEach(s -> System.out.println(" Bundle: " + s));
        File extraBundlesDir = new File(TestUtils.fixPathOnWindows(ExtensionController
                                                                           .class.getResource(
                "/sample-bundles2").getFile()));
        File[] files = extraBundlesDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (files != null) {
            for (File file : files) {
                try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
                    ExtensionKey extension = osgiController.addExtension(file.getName(),
                                                                         inputStream);
                    osgiController.updateStatus(extension, true);
                }
            }
        }

        interceptors = StreamSupport.stream(osgiController.getServicesWithInfo(
                CommandRequestInterceptor.class).spliterator(), false).sorted(
                Comparator.comparing(ServiceWithInfo::order)).collect(Collectors.toList());
        for (ServiceWithInfo<CommandRequestInterceptor> interceptor : interceptors) {
            System.out.println(interceptor.extensionKey());
            command = interceptor.service().commandRequest(command, unitOfWork);
        }
        System.out.println(command);
    }
}