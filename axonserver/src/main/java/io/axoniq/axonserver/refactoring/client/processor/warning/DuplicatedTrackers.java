/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.processor.warning;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 22/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DuplicatedTrackers implements Warning {

    private final Iterable<SegmentStatus> trackerInfos;

    public DuplicatedTrackers(Iterable<SegmentStatus> trackerInfos) {
        this.trackerInfos = trackerInfos;
    }

    @Override
    public boolean active() {
        int count = 0;
        Set<Integer> ids = new HashSet<>();

        for (SegmentStatus info : trackerInfos) {
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
