/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
 * Representation of a segment of a Streaming Event Processor.
 *
 * @author Sara Pellegrini
 */
public class StreamingProcessorSegment implements Printable {

    /**
     * The identifier of the client that claimed the segment.
     */
    private final String clientId;
    private final SegmentStatus segmentStatus;

    /**
     * Constructs a {@link StreamingProcessorSegment}.
     *
     * @param clientId      identifier of the client that claimed the segment
     * @param segmentStatus the status of the segment represented by this {@link Printable}
     */
    public StreamingProcessorSegment(String clientId, SegmentStatus segmentStatus) {
        this.clientId = clientId;
        this.segmentStatus = segmentStatus;
    }

    /**
     * The client identifier of this segment.
     *
     * @return the client identifier of this segment
     */
    public String clientId() {
        return clientId;
    }

    /**
     * The segment identifier.
     *
     * @return the segment identifier
     */
    public int segmentId() {
        return segmentStatus.getSegmentId();
    }

    @Override
    public void printOn(Media media) {
        media.with("clientId", clientId)
             .with("segmentId", segmentStatus.getSegmentId())
             .with("caughtUp", segmentStatus.getCaughtUp())
             .with("replaying", segmentStatus.getReplaying())
             .with("tokenPosition", segmentStatus.getTokenPosition())
             .with("errorState", segmentStatus.getErrorState())
             .with("onePartOf", segmentStatus.getOnePartOf());
    }
}
