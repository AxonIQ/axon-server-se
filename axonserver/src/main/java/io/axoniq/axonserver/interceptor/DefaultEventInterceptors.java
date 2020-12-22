/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.Ordered;
import io.axoniq.axonserver.extensions.OsgiController;
import io.axoniq.axonserver.extensions.hook.PostCommitEventsHook;
import io.axoniq.axonserver.extensions.hook.PostCommitSnapshotHook;
import io.axoniq.axonserver.extensions.hook.PreCommitEventsHook;
import io.axoniq.axonserver.extensions.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.AppendSnapshotInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadSnapshotInterceptor;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
    private final List<PreCommitEventsHook> eventsPreCommitInterceptors = new CopyOnWriteArrayList<>();
    private final List<PostCommitEventsHook> eventsPostCommitInterceptors = new CopyOnWriteArrayList<>();
    private final List<AppendSnapshotInterceptor> snapshotPreCommitInterceptors = new CopyOnWriteArrayList<>();
    private final List<PostCommitSnapshotHook> snapshotPostCommitInterceptors = new CopyOnWriteArrayList<>();
    private final List<ReadEventInterceptor> eventReadInterceptors = new CopyOnWriteArrayList<>();
    private final List<ReadSnapshotInterceptor> snapshotReadInterceptors = new CopyOnWriteArrayList<>();
    private final OsgiController osgiController;
    private volatile boolean initialized;

    public DefaultEventInterceptors(
            OsgiController osgiController) {
        this.osgiController = osgiController;
        osgiController.registerExtensionListener(serviceEvent -> {
            logger.debug("service event {}", serviceEvent.getLocation());
            initialized = false;
        });
    }

    private void ensureInitialized() {
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
                snapshotPostCommitInterceptors.clear();

                osgiController.getServices(AppendEventInterceptor.class).forEach(appendEventInterceptors::add);
                osgiController.getServices(PreCommitEventsHook.class).forEach(eventsPreCommitInterceptors::add);
                osgiController.getServices(ReadEventInterceptor.class).forEach(eventReadInterceptors::add);
                osgiController.getServices(PostCommitEventsHook.class)
                              .forEach(eventsPostCommitInterceptors::add);
                osgiController.getServices(ReadSnapshotInterceptor.class).forEach(snapshotReadInterceptors::add);
                osgiController.getServices(AppendSnapshotInterceptor.class)
                              .forEach(snapshotPreCommitInterceptors::add);
                osgiController.getServices(PostCommitSnapshotHook.class)
                              .forEach(snapshotPostCommitInterceptors::add);

                appendEventInterceptors.sort(Comparator.comparingInt(Ordered::order));
                eventsPreCommitInterceptors.sort(Comparator.comparingInt(Ordered::order));
                eventsPostCommitInterceptors.sort(Comparator.comparingInt(Ordered::order));
                snapshotPostCommitInterceptors.sort(Comparator.comparingInt(Ordered::order));
                eventReadInterceptors.sort(Comparator.comparingInt(Ordered::order));
                snapshotReadInterceptors.sort(Comparator.comparingInt(Ordered::order));
                snapshotPreCommitInterceptors.sort(Comparator.comparingInt(Ordered::order));

                initialized = true;

                logger.debug("{} appendEventInterceptors", appendEventInterceptors.size());
                logger.debug("{} eventsPreCommitInterceptors", eventsPreCommitInterceptors.size());
                logger.debug("{} eventsPostCommitInterceptors", eventsPostCommitInterceptors.size());
                logger.debug("{} eventReadInterceptors", eventReadInterceptors.size());
                logger.debug("{} snapshotReadInterceptors", snapshotReadInterceptors.size());
                logger.debug("{} snapshotPreCommitInterceptors", snapshotPreCommitInterceptors.size());
            }
        }
    }

    @Override
    public Event appendEvent(
            Event event, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        if (appendEventInterceptors.isEmpty()) {
            return event;
        }
        for (AppendEventInterceptor preCommitInterceptor : appendEventInterceptors) {
            event = preCommitInterceptor.appendEvent(event, interceptorContext);
        }
        return event;
    }

    @Override
    public void eventsPreCommit(List<Event> events,
                                ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (PreCommitEventsHook preCommitInterceptor : eventsPreCommitInterceptors) {
            preCommitInterceptor.onPreCommitEvents(events, interceptorContext);
        }
    }

    @Override
    public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (PostCommitEventsHook postCommitInterceptor : eventsPostCommitInterceptors) {
            postCommitInterceptor.onPostCommitEvent(events, interceptorContext);
        }
    }

    @Override
    public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (PostCommitSnapshotHook postCommitInterceptor : snapshotPostCommitInterceptors) {
            postCommitInterceptor.onPostCommitSnapshot(snapshot, interceptorContext);
        }
    }

    @Override
    public Event appendSnapshot(Event event, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (AppendSnapshotInterceptor snapshotPreCommitInterceptor : snapshotPreCommitInterceptors) {
            event = snapshotPreCommitInterceptor.appendSnapshot(event, interceptorContext);
        }
        return event;
    }

    @Override
    public boolean noReadInterceptors() {
        ensureInitialized();
        return eventReadInterceptors.isEmpty() && snapshotReadInterceptors.isEmpty();
    }

    @Override
    public Event readSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (ReadSnapshotInterceptor snapshotReadInterceptor : snapshotReadInterceptors) {
            snapshot = snapshotReadInterceptor.readSnapshot(snapshot, interceptorContext);
        }
        return snapshot;
    }

    @Override
    public Event readEvent(Event event, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (ReadEventInterceptor eventReadInterceptor : eventReadInterceptors) {
            event = eventReadInterceptor.readEvent(event, interceptorContext);
        }
        return event;
    }

    @Override
    public boolean noEventReadInterceptors() {
        ensureInitialized();
        return eventReadInterceptors.isEmpty();
    }
}
