/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.api;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * Fake implementation of {@link EventProcessorInstance}
 *
 * @author Sara Pellegrini
 */
public class FakeEvenProcessorInstance implements EventProcessorInstance {

    private final String clientId;
    private final boolean running;
    private final int maxSegments;
    private final List<EventProcessorSegment> segmentList;

    public FakeEvenProcessorInstance(String clientId, boolean running, int maxSegments,
                                     List<EventProcessorSegment> segmentList) {
        this.clientId = clientId;
        this.running = running;
        this.maxSegments = maxSegments;
        this.segmentList = segmentList;
    }

    @Nonnull
    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int maxCapacity() {
        return maxSegments;
    }

    @Nonnull
    @Override
    public Iterable<EventProcessorSegment> claimedSegments() {
        return segmentList;
    }
}
