/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 * @since
 */
@Component
public class ContextLimitProviderDefault implements ContextLimitProvider {

    @Override
    public Integer maxPendingCommands(String context) {
        return null;
    }
}
