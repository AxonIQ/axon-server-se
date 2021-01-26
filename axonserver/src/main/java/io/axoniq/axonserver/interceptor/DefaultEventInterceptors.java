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
import io.axoniq.axonserver.extensions.RequestRejectedException;
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

import java.util.Collections;
import java.util.List;

/**
 * Bundles all the interceptors for events and snapshots in a single component.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class DefaultEventInterceptors implements EventInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultEventInterceptors.class);
    private final ExtensionContextFilter extensionContextFilter;

    public DefaultEventInterceptors(
            ExtensionContextFilter extensionContextFilter) {
        this.extensionContextFilter = extensionContextFilter;
    }

    @Override
    public Event appendEvent(
            Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        Event intercepted = event;
        for (AppendEventInterceptor preCommitInterceptor : extensionContextFilter.getServicesForContext(
                AppendEventInterceptor.class,
                extensionUnitOfWork
                        .context())) {
            intercepted = preCommitInterceptor.appendEvent(intercepted, extensionUnitOfWork);
        }
        return mergeEvent(event, intercepted);
    }

    @Override
    public void eventsPreCommit(List<Event> events,
                                ExtensionUnitOfWork extensionUnitOfWork) throws RequestRejectedException {
        List<PreCommitEventsHook> servicesForContext = extensionContextFilter.getServicesForContext(
                PreCommitEventsHook.class,
                extensionUnitOfWork.context());
        if (servicesForContext.isEmpty()) {
            return;
        }

        List<Event> immutableEvents = Collections.unmodifiableList(events);
        for (PreCommitEventsHook preCommitEventsHook : servicesForContext) {
            preCommitEventsHook.onPreCommitEvents(immutableEvents, extensionUnitOfWork);
        }
    }

    @Override
    public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork extensionUnitOfWork) {
        try {
            List<PostCommitEventsHook> servicesForContext = extensionContextFilter.getServicesForContext(
                    PostCommitEventsHook.class,
                    extensionUnitOfWork.context());
            if (servicesForContext.isEmpty()) {
                return;
            }
            List<Event> immutableList = Collections.unmodifiableList(events);
            for (PostCommitEventsHook postCommitEventsHook : servicesForContext) {
                postCommitEventsHook.onPostCommitEvent(immutableList, extensionUnitOfWork);
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a PostCommitEventsHook", extensionUnitOfWork.principal(),
                        extensionUnitOfWork.context(), ex);
        }
    }

    @Override
    public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        try {
            for (PostCommitSnapshotHook postCommitSnapshotHook : extensionContextFilter.getServicesForContext(
                    PostCommitSnapshotHook.class,
                    extensionUnitOfWork.context())) {
                postCommitSnapshotHook.onPostCommitSnapshot(snapshot, extensionUnitOfWork);
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a PostCommitSnapshotHook", extensionUnitOfWork.principal(),
                        extensionUnitOfWork.context(), ex);
        }
    }

    @Override
    public Event appendSnapshot(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        Event intercepted = snapshot;
        for (AppendSnapshotInterceptor appendSnapshotInterceptor : extensionContextFilter.getServicesForContext(
                AppendSnapshotInterceptor.class,
                extensionUnitOfWork.context())) {
            try {
                intercepted = appendSnapshotInterceptor.appendSnapshot(intercepted, extensionUnitOfWork);
            } catch (RequestRejectedException e) {
                e.printStackTrace();
            }
        }
        return mergeEvent(snapshot, intercepted);
    }

    @Override
    public boolean noReadInterceptors(String context) {
        return noSnapshotReadInterceptors(context) && noEventReadInterceptors(context);
    }

    @Override
    public Event readSnapshot(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        Event intercepted = snapshot;
        for (ReadSnapshotInterceptor snapshotReadInterceptor : extensionContextFilter.getServicesForContext(
                ReadSnapshotInterceptor.class,
                extensionUnitOfWork.context()
        )) {
            intercepted = snapshotReadInterceptor.readSnapshot(intercepted, extensionUnitOfWork);
        }
        return mergeEvent(snapshot, intercepted);
    }

    @Override
    public Event readEvent(Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        Event intercepted = event;
        for (ReadEventInterceptor eventReadInterceptor : extensionContextFilter.getServicesForContext(
                ReadEventInterceptor.class,
                extensionUnitOfWork.context())) {
            intercepted = eventReadInterceptor.readEvent(intercepted, extensionUnitOfWork);
        }
        return mergeEvent(event, intercepted);
    }

    private Event mergeEvent(Event original, Event intercepted) {
        return Event.newBuilder(original)
                    .setPayload(intercepted.getPayload())
                    .clearMetaData()
                    .putAllMetaData(intercepted.getMetaDataMap())
                    .build();
    }

    @Override
    public boolean noEventReadInterceptors(String context) {
        return extensionContextFilter.getServicesForContext(ReadEventInterceptor.class, context).isEmpty();
    }

    @Override
    public boolean noSnapshotReadInterceptors(String context) {
        return extensionContextFilter.getServicesForContext(ReadSnapshotInterceptor.class, context).isEmpty();
    }
}
