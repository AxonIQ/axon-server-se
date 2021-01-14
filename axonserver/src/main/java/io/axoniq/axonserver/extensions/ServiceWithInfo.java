/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

/**
 * @author Marc Gathier
 */
public class ServiceWithInfo<T extends Ordered> {

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
}
