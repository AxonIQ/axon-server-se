/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.util.ObjectUtils;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultEventInterceptors implements EventInterceptors {

    private final List<EventPreCommitInterceptor> eventPreCommitInterceptors;
    private final List<SnapshotPreCommitInterceptor> snapshotPreCommitInterceptors;
    private final List<EventReadInterceptor> eventReadInterceptors;
    private final List<SnapshotReadInterceptor> snapshotReadInterceptors;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DefaultEventInterceptors(
            @Nullable List<EventPreCommitInterceptor> eventPreCommitInterceptors,
            @Nullable List<SnapshotPreCommitInterceptor> snapshotPreCommitInterceptors,
            @Nullable List<EventReadInterceptor> eventReadInterceptors,
            @Nullable List<SnapshotReadInterceptor> snapshotReadInterceptors) {
        this.eventPreCommitInterceptors = ObjectUtils.getOrDefault(eventPreCommitInterceptors, Collections.emptyList());
        this.snapshotPreCommitInterceptors = ObjectUtils.getOrDefault(snapshotPreCommitInterceptors,
                                                                      Collections.emptyList());
        this.eventReadInterceptors = ObjectUtils.getOrDefault(eventReadInterceptors, Collections.emptyList());
        this.snapshotReadInterceptors = ObjectUtils.getOrDefault(snapshotReadInterceptors, Collections.emptyList());
    }

    @Override
    public InputStream eventPreCommit(
            InterceptorContext interceptorContext, InputStream eventInputStream) {
        if (eventPreCommitInterceptors.isEmpty()) {
            return eventInputStream;
        }
        try {
            Event event = Event.parseFrom(eventInputStream);
            for (EventPreCommitInterceptor preCommitInterceptor : eventPreCommitInterceptors) {
                event = preCommitInterceptor.eventPreCommit(interceptorContext, event);
            }
            return new ByteArrayInputStream(event.toByteArray());
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Could not parse event from client", ioException);
        }
    }

    @Override
    public Event snapshotPreRequest(InterceptorContext interceptorContext, Event event) {
        for (SnapshotPreCommitInterceptor snapshotPreCommitInterceptor : snapshotPreCommitInterceptors) {
            event = snapshotPreCommitInterceptor.snapshotPreCommit(interceptorContext, event);
        }
        return event;
    }

    @Override
    public boolean noReadInterceptors() {
        return eventReadInterceptors.isEmpty() && snapshotReadInterceptors.isEmpty();
    }

    @Override
    public Event readSnapshot(InterceptorContext interceptorContext, Event snapshot) {
        for (SnapshotReadInterceptor snapshotReadInterceptor : snapshotReadInterceptors) {
            snapshot = snapshotReadInterceptor.readSnapshot(interceptorContext, snapshot);
        }
        return snapshot;
    }

    @Override
    public Event readEvent(InterceptorContext interceptorContext, Event event) {
        for (EventReadInterceptor eventReadInterceptor : eventReadInterceptors) {
            event = eventReadInterceptor.readEvent(interceptorContext, event);
        }
        return event;
    }

    @Override
    public boolean noEventReadInterceptors() {
        return eventReadInterceptors.isEmpty();
    }
}
