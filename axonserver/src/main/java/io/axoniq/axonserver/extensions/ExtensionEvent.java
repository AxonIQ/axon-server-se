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
public class ExtensionEvent {

    private final ExtensionKey bundleInfo;

    public ExtensionEvent(ExtensionKey bundleInfo) {
        this.bundleInfo = bundleInfo;
    }

    public ExtensionKey getBundleInfo() {
        return bundleInfo;
    }
}
