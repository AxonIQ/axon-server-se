/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Representation of a segment of a Streaming Event Processor.
 *
 * @author Sara Pellegrini
 */
public class StreamingProcessorSegment implements Printable {

    private final EventProcessorSegment eventProcessorSegment;

    /**
     * Creates an instance based on {@link EventProcessorSegment}.
     *
     * @param eventProcessorSegment the source of information about segment.
     */
    public StreamingProcessorSegment(EventProcessorSegment eventProcessorSegment) {
        this.eventProcessorSegment = eventProcessorSegment;
    }

    @Override
    public void printOn(Media media) {
        media.with("clientId", eventProcessorSegment.claimedBy())
             .with("segmentId", eventProcessorSegment.id())
             .with("caughtUp", eventProcessorSegment.isCaughtUp())
             .with("replaying", eventProcessorSegment.isReplaying())
             .with("tokenPosition", eventProcessorSegment.tokenPosition())
             .with("errorState", eventProcessorSegment.error().orElse(""))
             .with("onePartOf", eventProcessorSegment.onePartOf());
    }
}
