/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 */
public class ServiceWithInfo<T extends Ordered> implements Comparable<ServiceWithInfo<T>> {

    private final T service;
    private final ExtensionKey extensionKey;

    public ServiceWithInfo(T service, ExtensionKey extensionKey) {
        this.service = service;
        this.extensionKey = extensionKey;
    }

    public T service() {
        return service;
    }

    public ExtensionKey extensionKey() {
        return extensionKey;
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
