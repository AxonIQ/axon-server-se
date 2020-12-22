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
 * Bundles all the interceptors for events and snapshots in a single component.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class DefaultEventInterceptors implements EventInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultEventInterceptors.class);
    private final List<AppendEventInterceptor> appendEventInterceptors = new CopyOnWriteArrayList<>();
    private final List<PreCommitEventsHook> preCommitEventsHooks = new CopyOnWriteArrayList<>();
    private final List<PostCommitEventsHook> postCommitEventsHooks = new CopyOnWriteArrayList<>();
    private final List<AppendSnapshotInterceptor> appendSnapshotInterceptors = new CopyOnWriteArrayList<>();
    private final List<PostCommitSnapshotHook> postCommitSnapshotHooks = new CopyOnWriteArrayList<>();
    private final List<ReadEventInterceptor> readEventInterceptors = new CopyOnWriteArrayList<>();
    private final List<ReadSnapshotInterceptor> readSnapshotInterceptors = new CopyOnWriteArrayList<>();
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
                preCommitEventsHooks.clear();
                postCommitEventsHooks.clear();
                readEventInterceptors.clear();
                readSnapshotInterceptors.clear();
                appendSnapshotInterceptors.clear();
                postCommitSnapshotHooks.clear();

                osgiController.getServices(AppendEventInterceptor.class).forEach(appendEventInterceptors::add);
                osgiController.getServices(PreCommitEventsHook.class).forEach(preCommitEventsHooks::add);
                osgiController.getServices(ReadEventInterceptor.class).forEach(readEventInterceptors::add);
                osgiController.getServices(PostCommitEventsHook.class)
                              .forEach(postCommitEventsHooks::add);
                osgiController.getServices(ReadSnapshotInterceptor.class).forEach(readSnapshotInterceptors::add);
                osgiController.getServices(AppendSnapshotInterceptor.class)
                              .forEach(appendSnapshotInterceptors::add);
                osgiController.getServices(PostCommitSnapshotHook.class)
                              .forEach(postCommitSnapshotHooks::add);

                appendEventInterceptors.sort(Comparator.comparingInt(Ordered::order));
                preCommitEventsHooks.sort(Comparator.comparingInt(Ordered::order));
                postCommitEventsHooks.sort(Comparator.comparingInt(Ordered::order));
                postCommitSnapshotHooks.sort(Comparator.comparingInt(Ordered::order));
                readEventInterceptors.sort(Comparator.comparingInt(Ordered::order));
                readSnapshotInterceptors.sort(Comparator.comparingInt(Ordered::order));
                appendSnapshotInterceptors.sort(Comparator.comparingInt(Ordered::order));

                initialized = true;

                logger.debug("{} appendEventInterceptors", appendEventInterceptors.size());
                logger.debug("{} preCommitEventsHooks", preCommitEventsHooks.size());
                logger.debug("{} postCommitEventsHooks", postCommitEventsHooks.size());
                logger.debug("{} readEventInterceptors", readEventInterceptors.size());
                logger.debug("{} readSnapshotInterceptors", readSnapshotInterceptors.size());
                logger.debug("{} appendSnapshotInterceptors", appendSnapshotInterceptors.size());
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
        for (PreCommitEventsHook preCommitEventsHook : preCommitEventsHooks) {
            preCommitEventsHook.onPreCommitEvents(events, interceptorContext);
        }
    }

    @Override
    public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        try {
            for (PostCommitEventsHook postCommitEventsHook : postCommitEventsHooks) {
                postCommitEventsHook.onPostCommitEvent(events, interceptorContext);
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a PostCommitEventsHook", interceptorContext.principal(),
                        interceptorContext.context(), ex);
        }
    }

    @Override
    public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        try {
            for (PostCommitSnapshotHook postCommitSnapshotHook : postCommitSnapshotHooks) {
                postCommitSnapshotHook.onPostCommitSnapshot(snapshot, interceptorContext);
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a PostCommitSnapshotHook", interceptorContext.principal(),
                        interceptorContext.context(), ex);
        }
    }

    @Override
    public Event appendSnapshot(Event event, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (AppendSnapshotInterceptor appendSnapshotInterceptor : appendSnapshotInterceptors) {
            event = appendSnapshotInterceptor.appendSnapshot(event, interceptorContext);
        }
        return event;
    }

    @Override
    public boolean noReadInterceptors() {
        ensureInitialized();
        return readEventInterceptors.isEmpty() && readSnapshotInterceptors.isEmpty();
    }

    @Override
    public Event readSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (ReadSnapshotInterceptor snapshotReadInterceptor : readSnapshotInterceptors) {
            snapshot = snapshotReadInterceptor.readSnapshot(snapshot, interceptorContext);
        }
        return snapshot;
    }

    @Override
    public Event readEvent(Event event, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (ReadEventInterceptor eventReadInterceptor : readEventInterceptors) {
            event = eventReadInterceptor.readEvent(event, interceptorContext);
        }
        return event;
    }

    @Override
    public boolean noEventReadInterceptors() {
        ensureInitialized();
        return readEventInterceptors.isEmpty();
    }
}
