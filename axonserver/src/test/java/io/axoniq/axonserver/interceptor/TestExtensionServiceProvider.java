/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionKey;
import io.axoniq.axonserver.extensions.ExtensionServiceProvider;
import io.axoniq.axonserver.extensions.Ordered;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.localstorage.Registration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * @author Marc Gathier
 */
class TestExtensionServiceProvider implements ExtensionServiceProvider {

    private final List<ServiceWithInfo<? extends Ordered>> services = new ArrayList<>();
    private final Set<BiConsumer<ExtensionKey, String>> listeners = new HashSet<>();

    public void add(ServiceWithInfo<? extends Ordered> service) {
        services.add(service);
        listeners.forEach(c -> c.accept(service.extensionKey(), "Running"));
    }

    @Override
    public Registration registerExtensionListener(BiConsumer<ExtensionKey, String> listener) {
        listeners.add(listener);
        return null;
    }

    @Override
    public <T extends Ordered> Set<ServiceWithInfo<T>> getServicesWithInfo(Class<T> clazz) {
        HashSet<ServiceWithInfo<T>> result = new HashSet<>();
        for (ServiceWithInfo<? extends Ordered> service : services) {
            if (clazz.isInstance(service.service())) {
                result.add((ServiceWithInfo<T>) service);
            }
        }
        return result;
    }
}
