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
 * Created by Sara Pellegrini on 22/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DuplicatedTrackers implements Warning {

    private final Iterable<EventTrackerInfo> trackerInfos;

    public DuplicatedTrackers(Iterable<EventTrackerInfo> trackerInfos) {
        this.trackerInfos = trackerInfos;
    }

    @Override
    public boolean active() {
        int count = 0;
        Set<Integer> ids = new HashSet<>();

        for (EventTrackerInfo info : trackerInfos) {
            ids.add(info.getSegmentId());
            count++;
        }

        return count != ids.size();
    }

    @Override
    public String message() {
        return "Duplicated segment claim detected";
    }

}
