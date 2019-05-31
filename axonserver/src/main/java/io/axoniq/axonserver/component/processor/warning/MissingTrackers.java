/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.warning;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo.EventTrackerInfo;

import java.util.HashSet;
import java.util.Set;

/**
 * {@link Warning} implementation that activates when the trackers for a event processor have not covered all segments.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class MissingTrackers implements Warning {

    private final Iterable<EventTrackerInfo> trackerInfos;

    public MissingTrackers(Iterable<EventTrackerInfo> trackerInfos) {
        this.trackerInfos = trackerInfos;
    }

    @Override
    public boolean active() {
        double completion = 0;
        Set<Integer> ids = new HashSet<>();

        for (EventTrackerInfo info : trackerInfos) {
            int segmentId = info.getSegmentId();
            if (!ids.contains(segmentId))completion += 1d/info.getOnePartOf();
            ids.add(segmentId);
        }

        return completion < 1;
    }

    @Override
    public String message() {
        return "Not all segments claimed";
    }

}
