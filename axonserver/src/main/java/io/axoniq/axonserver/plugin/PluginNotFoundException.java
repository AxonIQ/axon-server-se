/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;

/**
 * Exception thrown when a plugin is not found.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class PluginNotFoundException extends MessagingPlatformException {

    public PluginNotFoundException(String extension, String version) {
        super(ErrorCode.PLUGIN_NOT_FOUND, String.format("Plugin not found: %s/%s", extension, version));
    }
}
