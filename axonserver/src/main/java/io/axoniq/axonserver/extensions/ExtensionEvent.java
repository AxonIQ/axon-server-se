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
 * Event published when an extension is installed/removed.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ExtensionEvent {

    private final ExtensionKey extension;
    private final String status;

    public ExtensionEvent(ExtensionKey extension) {
        this(extension, null);
    }

    public ExtensionEvent(ExtensionKey extensionKey, String status) {
        this.extension = extensionKey;
        this.status = status;
    }

    public ExtensionKey getExtension() {
        return extension;
    }

    public String getStatus() {
        return status;
    }
}
