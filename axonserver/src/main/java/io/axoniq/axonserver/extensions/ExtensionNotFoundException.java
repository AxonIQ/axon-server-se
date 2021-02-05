/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;

/**
 * @author Marc Gathier
 */
public class ExtensionNotFoundException extends MessagingPlatformException {

    public ExtensionNotFoundException(String extension, String version) {
        super(ErrorCode.OTHER, String.format("Extension not found: %s/%s", extension, version));
    }
}
