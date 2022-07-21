/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json.warning;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 22/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DuplicatedTrackers implements Warning {

    private final Iterable<EventProcessorSegment> trackerInfos;

    public DuplicatedTrackers(Iterable<EventProcessorSegment> trackerInfos) {
        this.trackerInfos = trackerInfos;
    }

    @Override
    public boolean active() {
        int count = 0;
        Set<Integer> ids = new HashSet<>();

        for (EventProcessorSegment info : trackerInfos) {
            ids.add(info.id());
            count++;
        }

        return count != ids.size();
    }

    @Override
    public String message() {
        return "Duplicated segment claim detected";
    }
}
