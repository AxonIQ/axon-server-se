/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocessing.spi;

/**
 * Marker interface for Command Processing interceptors.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public interface Interceptor {

    int PRIORITY_FIRST = Integer.MIN_VALUE;
    int PRIORITY_LAST = Integer.MAX_VALUE;
    int PRIORITY_LATER = 100;
    int PRIORITY_EARLIER = -100;

    default int priority() {
        return 0;
    }
}
