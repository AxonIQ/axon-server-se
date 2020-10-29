/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.config.OsgiController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.EventReadInterceptor;
import io.axoniq.axonserver.extensions.interceptor.EventsPostCommitInterceptor;
import io.axoniq.axonserver.extensions.interceptor.EventsPreCommitInterceptor;
import io.axoniq.axonserver.extensions.interceptor.InterceptorContext;
import io.axoniq.axonserver.extensions.interceptor.OrderedInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SnapshotPreCommitInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SnapshotReadInterceptor;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultEventInterceptors implements EventInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultEventInterceptors.class);
    private final List<AppendEventInterceptor> appendEventInterceptors = new CopyOnWriteArrayList<>();
    private final List<EventsPreCommitInterceptor> eventsPreCommitInterceptors = new CopyOnWriteArrayList<>();
    private final List<EventsPostCommitInterceptor> eventsPostCommitInterceptors = new CopyOnWriteArrayList<>();
    private final List<SnapshotPreCommitInterceptor> snapshotPreCommitInterceptors = new CopyOnWriteArrayList<>();
    private final List<EventReadInterceptor> eventReadInterceptors = new CopyOnWriteArrayList<>();
    private final List<SnapshotReadInterceptor> snapshotReadInterceptors = new CopyOnWriteArrayList<>();
    private final OsgiController osgiController;
    private volatile boolean initialized;

    public DefaultEventInterceptors(
            OsgiController osgiController) {
        this.osgiController = osgiController;
    }

    private void initialize() {
        if (!initialized) {
            synchronized (osgiController) {
                if (initialized) {
                    return;
                }

                appendEventInterceptors.clear();
                eventsPreCommitInterceptors.clear();
                eventsPostCommitInterceptors.clear();
                eventReadInterceptors.clear();
                snapshotReadInterceptors.clear();
                snapshotPreCommitInterceptors.clear();

                osgiController.getServices(AppendEventInterceptor.class).forEach(appendEventInterceptors::add);
                osgiController.getServices(EventsPreCommitInterceptor.class).forEach(eventsPreCommitInterceptors::add);
                osgiController.getServices(EventReadInterceptor.class).forEach(eventReadInterceptors::add);
                osgiController.getServices(EventsPostCommitInterceptor.class)
                              .forEach(eventsPostCommitInterceptors::add);
                osgiController.getServices(SnapshotReadInterceptor.class).forEach(snapshotReadInterceptors::add);
                osgiController.getServices(SnapshotPreCommitInterceptor.class)
                              .forEach(snapshotPreCommitInterceptors::add);

                appendEventInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                eventsPreCommitInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                eventsPostCommitInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                eventReadInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                snapshotReadInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                snapshotPreCommitInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));

                initialized = true;

                logger.warn("{} appendEventInterceptors", appendEventInterceptors.size());
                logger.warn("{} eventsPreCommitInterceptors", eventsPreCommitInterceptors.size());
                logger.warn("{} eventsPostCommitInterceptors", eventsPostCommitInterceptors.size());
                logger.warn("{} eventReadInterceptors", eventReadInterceptors.size());
                logger.warn("{} snapshotReadInterceptors", snapshotReadInterceptors.size());
                logger.warn("{} snapshotPreCommitInterceptors", snapshotPreCommitInterceptors.size());

                osgiController.registerServiceListener(serviceEvent -> {
                    logger.warn("service event {}", serviceEvent.getLocation());
                    initialized = false;
                });
            }
        }
    }

    @Override
    public InputStream appendEvent(
            InterceptorContext interceptorContext, InputStream eventInputStream) {
        initialize();
        if (appendEventInterceptors.isEmpty()) {
            return eventInputStream;
        }
        try {
            Event event = Event.parseFrom(eventInputStream);
            for (AppendEventInterceptor preCommitInterceptor : appendEventInterceptors) {
                event = preCommitInterceptor.appendEvent(interceptorContext, event);
            }
            return new ByteArrayInputStream(event.toByteArray());
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Could not parse event from client", ioException);
        }
    }

    @Override
    public void eventsPreCommit(InterceptorContext interceptorContext) {
        initialize();
        for (EventsPreCommitInterceptor preCommitInterceptor : eventsPreCommitInterceptors) {
            preCommitInterceptor.eventsPreCommit(interceptorContext);
        }
    }

    @Override
    public void eventsPostCommit(InterceptorContext interceptorContext) {
        initialize();
        for (EventsPostCommitInterceptor postCommitInterceptor : eventsPostCommitInterceptors) {
            postCommitInterceptor.eventsPreCommit(interceptorContext);
        }
    }

    @Override
    public Event snapshotPreRequest(InterceptorContext interceptorContext, Event event) {
        initialize();
        for (SnapshotPreCommitInterceptor snapshotPreCommitInterceptor : snapshotPreCommitInterceptors) {
            event = snapshotPreCommitInterceptor.snapshotPreCommit(interceptorContext, event);
        }
        return event;
    }

    @Override
    public boolean noReadInterceptors() {
        initialize();
        return eventReadInterceptors.isEmpty() && snapshotReadInterceptors.isEmpty();
    }

    @Override
    public Event readSnapshot(InterceptorContext interceptorContext, Event snapshot) {
        initialize();
        for (SnapshotReadInterceptor snapshotReadInterceptor : snapshotReadInterceptors) {
            snapshot = snapshotReadInterceptor.readSnapshot(interceptorContext, snapshot);
        }
        return snapshot;
    }

    @Override
    public Event readEvent(InterceptorContext interceptorContext, Event event) {
        initialize();
        for (EventReadInterceptor eventReadInterceptor : eventReadInterceptors) {
            event = eventReadInterceptor.readEvent(interceptorContext, event);
        }
        return event;
    }

    @Override
    public boolean noEventReadInterceptors() {
        initialize();
        return eventReadInterceptors.isEmpty();
    }
}
