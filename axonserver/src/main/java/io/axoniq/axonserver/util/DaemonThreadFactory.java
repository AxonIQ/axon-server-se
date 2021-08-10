/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 * Thread factory to create daemon threads.
 * @author Marc Gathier
 * @since 4.6.0
 */
public class DaemonThreadFactory extends CustomizableThreadFactory {

    public DaemonThreadFactory(String s) {
        super(s);
        setDaemon(true);
    }
}
