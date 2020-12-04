/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.Ordered;
import io.axoniq.axonserver.extensions.OsgiController;
import io.axoniq.axonserver.extensions.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.interceptor.DefaultInterceptorContext;
import io.axoniq.axonserver.test.TestUtils;
import org.junit.*;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.metatype.AttributeDefinition;
import org.osgi.service.metatype.MetaTypeInformation;
import org.osgi.service.metatype.MetaTypeService;
import org.osgi.service.metatype.ObjectClassDefinition;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author Marc Gathier
 */
public class OsgiControllerTest {

    @Test
    public void start() throws IOException, BundleException {
        OsgiController osgiController = new OsgiController(TestUtils.fixPathOnWindows(OsgiController
                                                                                              .class.getResource(
                "/sample-bundles")
                                                                                                    .getFile()),
                                                           "4.5.0");
        osgiController.start();
        MetaTypeService metaTypeService = osgiController.getService(MetaTypeService.class);
        int idx = 1;
        Bundle bundle = osgiController.getBundle(idx);
        while (bundle != null) {
            MetaTypeInformation information = metaTypeService.getMetaTypeInformation(bundle);
            System.out.printf("%s/%s - %s%n", bundle.getSymbolicName(),
                              bundle.getVersion(),
                              String.join(",", information.getPids()));

            for (String pid : information.getPids()) {
                ObjectClassDefinition objectClassDefinition = information
                        .getObjectClassDefinition(pid, null);
                for (AttributeDefinition attributeDefinition : objectClassDefinition.getAttributeDefinitions(-1)) {
                    System.out.printf("\t%s=%s%n",
                                      attributeDefinition.getName(),
                                      attributeDefinition.getDefaultValue());
                }
            }
            idx++;
            bundle = osgiController.getBundle(idx);
        }

        ConfigurationAdmin configurationAdmin = osgiController.getService(ConfigurationAdmin.class);
        Configuration config = configurationAdmin
                .getConfiguration("org.sample.custom-interceptors2", "?");
        System.out.println(config.getProperties());
        Dictionary<String, Object> map = config.getProperties() == null ? new Hashtable<>() : config.getProperties();
        map.put("me.fileinstall.dir", "demoValueXXXXX");
        config.update(map);
        System.out.println(configurationAdmin.getConfiguration("org.sample.custom-interceptors2").getProperties());

        Command command = Command.newBuilder().setClientId("sample").build();
        List<CommandRequestInterceptor> interceptors = StreamSupport.stream(osgiController.getServices(
                CommandRequestInterceptor.class).spliterator(), false).sorted(
                Comparator.comparing(Ordered::order)).collect(Collectors.toList());

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
        for (CommandRequestInterceptor interceptor : interceptors) {
            command = interceptor.commandRequest(command, unitOfWork);
        }
        System.out.println(command);

        osgiController.listBundles().forEach(s -> System.out.println(" Bundle: " + s));
        File extraBundlesDir = new File(TestUtils.fixPathOnWindows(OsgiController
                                                                           .class.getResource(
                "/sample-bundles2").getFile()));
        File[] files = extraBundlesDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (files != null) {
            for (File file : files) {
                try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
                    osgiController.addExtension(file.getName(), inputStream);
                }
            }
        }

        interceptors = StreamSupport.stream(osgiController.getServices(
                CommandRequestInterceptor.class).spliterator(), false).sorted(
                Comparator.comparing(Ordered::order)).collect(Collectors.toList());
        for (CommandRequestInterceptor interceptor : interceptors) {
            command = interceptor.commandRequest(command, unitOfWork);
        }
        System.out.println(command);
    }
}