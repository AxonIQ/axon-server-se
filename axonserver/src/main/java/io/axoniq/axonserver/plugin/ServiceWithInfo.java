/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import javax.annotation.Nonnull;

/**
 * Wraps a service with information on the plugin where the service is defined.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ServiceWithInfo<T extends Ordered> implements Comparable<ServiceWithInfo<T>> {

    private final T service;
    private final PluginKey pluginKey;

    public ServiceWithInfo(T service, PluginKey pluginKey) {
        this.service = service;
        this.pluginKey = pluginKey;
    }

    public T service() {
        return service;
    }

    public PluginKey pluginKey() {
        return pluginKey;
    }

    public int order() {
        return service.order();
    }

    @Override
    public int compareTo(@Nonnull ServiceWithInfo<T> o) {
        if (o.order() == service.order()) {
            return service.getClass().getName().compareTo(o.service.getClass().getName());
        }
        return Integer.compare(service().order(), o.order());
    }
}
