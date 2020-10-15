/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.extensions.transform.EventTransformer;
import io.axoniq.axonserver.extensions.transform.EventTransformerFactory;

/**
 * @author Marc Gathier
 */
public class DefaultEventTransformerFactory implements EventTransformerFactory {

    @Override
    public EventTransformer get(byte version, int flags) {
        return NoOpEventTransformer.INSTANCE;
    }

}
