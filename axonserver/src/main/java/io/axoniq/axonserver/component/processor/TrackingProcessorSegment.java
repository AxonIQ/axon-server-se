/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public class TrackingProcessorSegment implements Printable {

    private final String clientId;

    private final SegmentStatus eventTrackerInfo;

    public TrackingProcessorSegment(String clientId, SegmentStatus eventTrackerInfo) {
        this.clientId = clientId;

        this.eventTrackerInfo = eventTrackerInfo;
    }

    public String clientId() {
        return clientId;
    }

    public int segmentId() {
        return eventTrackerInfo.getSegmentId();
    }

    @Override
    public void printOn(Media media) {
        media.with("clientId", clientId)
             .with("segmentId", eventTrackerInfo.getSegmentId())
             .with("caughtUp", eventTrackerInfo.getCaughtUp())
             .with("replaying", eventTrackerInfo.getReplaying())
             .with("tokenPosition", eventTrackerInfo.getTokenPosition())
             .with("errorState", eventTrackerInfo.getErrorState())
             .with("onePartOf", eventTrackerInfo.getOnePartOf());
    }
}
