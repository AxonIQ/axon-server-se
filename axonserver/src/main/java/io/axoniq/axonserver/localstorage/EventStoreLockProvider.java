/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.util.DefaultIdLock;
import io.axoniq.axonserver.util.IdLock;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Component
public class EventStoreLockProvider implements Function<String, IdLock> {

    private Map<String, IdLock> lockPerContext = new ConcurrentHashMap<>();

    @Override
    public IdLock apply(String context) {
        return lockPerContext.computeIfAbsent(context, c -> new DefaultIdLock());
    }

    public void remove(String context) {
        lockPerContext.remove(context);
    }
}
