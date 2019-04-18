/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.config.SystemInfoProvider;

import java.net.UnknownHostException;

/**
 * @author Marc Gathier
 */
public class TestSystemInfoProvider implements SystemInfoProvider {

    @Override
    public int getPort() {
        return 8024;
    }

    @Override
    public String getHostName() throws UnknownHostException {
        return "test";
    }
}
