/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.processor.listener;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import io.axoniq.axonserver.refactoring.client.ComponentItem;

import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface ClientProcessor extends ComponentItem, Iterable<SegmentStatus> {

    String clientId();

    EventProcessorInfo eventProcessorInfo();

    default Boolean running() {
        return eventProcessorInfo().getRunning();
    }

    @Nonnull
    @Override
    default Iterator<SegmentStatus> iterator() {
        return eventProcessorInfo().getSegmentStatusList().iterator();
    }
}
