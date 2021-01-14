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
import io.axoniq.axonserver.extensions.ServiceWithInfo;
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
    private final List<ServiceWithInfo<AppendEventInterceptor>> appendEventInterceptors = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<PreCommitEventsHook>> preCommitEventsHooks = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<PostCommitEventsHook>> postCommitEventsHooks = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<AppendSnapshotInterceptor>> appendSnapshotInterceptors = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<PostCommitSnapshotHook>> postCommitSnapshotHooks = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<ReadEventInterceptor>> readEventInterceptors = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<ReadSnapshotInterceptor>> readSnapshotInterceptors = new CopyOnWriteArrayList<>();
    private final OsgiController osgiController;
    private final ExtensionContextFilter extensionContextFilter;
    private volatile boolean initialized;

    public DefaultEventInterceptors(
            OsgiController osgiController,
            ExtensionContextFilter extensionContextFilter) {
        this.osgiController = osgiController;
        this.extensionContextFilter = extensionContextFilter;
        osgiController.registerExtensionListener(serviceEvent -> {
            logger.debug("extension event {}", serviceEvent.getLocation());
            initialized = false;
        });
    }


    private void ensureInitialized() {
        if (!initialized) {
            synchronized (osgiController) {
                if (initialized) {
                    return;
                }

                initHooks(AppendEventInterceptor.class, appendEventInterceptors);
                initHooks(PreCommitEventsHook.class, preCommitEventsHooks);
                initHooks(PostCommitEventsHook.class, postCommitEventsHooks);
                initHooks(AppendSnapshotInterceptor.class, appendSnapshotInterceptors);
                initHooks(PostCommitSnapshotHook.class, postCommitSnapshotHooks);
                initHooks(ReadEventInterceptor.class, readEventInterceptors);
                initHooks(ReadSnapshotInterceptor.class, readSnapshotInterceptors);

                initialized = true;
            }
        }
    }

    private <T extends Ordered> void initHooks(Class<T> clazz, List<ServiceWithInfo<T>> hooks) {
        hooks.clear();
        hooks.addAll(osgiController.getServicesWithInfo(clazz));
        hooks.sort(Comparator.comparingInt(ServiceWithInfo::order));
        logger.debug("{} {}}", hooks.size(), clazz.getSimpleName());
    }

    @Override
    public Event appendEvent(
            Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();

        for (ServiceWithInfo<AppendEventInterceptor> preCommitInterceptor : appendEventInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), preCommitInterceptor.extensionKey())) {
                event = preCommitInterceptor.service().appendEvent(event, extensionUnitOfWork);
            }
        }
        return event;
    }

    @Override
    public void eventsPreCommit(List<Event> events,
                                ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();

        for (ServiceWithInfo<PreCommitEventsHook> preCommitEventsHook : preCommitEventsHooks) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), preCommitEventsHook.extensionKey())) {
                preCommitEventsHook.service().onPreCommitEvents(events, extensionUnitOfWork);
            }
        }
    }

    @Override
    public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        try {
            for (ServiceWithInfo<PostCommitEventsHook> postCommitEventsHook : postCommitEventsHooks) {
                if (extensionContextFilter.test(extensionUnitOfWork.context(), postCommitEventsHook.extensionKey())) {
                    postCommitEventsHook.service().onPostCommitEvent(events, extensionUnitOfWork);
                }
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a PostCommitEventsHook", extensionUnitOfWork.principal(),
                        extensionUnitOfWork.context(), ex);
        }
    }

    @Override
    public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        try {
            for (ServiceWithInfo<PostCommitSnapshotHook> postCommitSnapshotHook : postCommitSnapshotHooks) {
                if (extensionContextFilter.test(extensionUnitOfWork.context(), postCommitSnapshotHook.extensionKey())) {
                    postCommitSnapshotHook.service().onPostCommitSnapshot(snapshot, extensionUnitOfWork);
                }
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a PostCommitSnapshotHook", extensionUnitOfWork.principal(),
                        extensionUnitOfWork.context(), ex);
        }
    }

    @Override
    public Event appendSnapshot(Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        for (ServiceWithInfo<AppendSnapshotInterceptor> appendSnapshotInterceptor : appendSnapshotInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), appendSnapshotInterceptor.extensionKey())) {
                event = appendSnapshotInterceptor.service().appendSnapshot(event, extensionUnitOfWork);
            }
        }
        return event;
    }

    @Override
    public boolean noReadInterceptors() {
        ensureInitialized();
        return readEventInterceptors.isEmpty() && readSnapshotInterceptors.isEmpty();
    }

    @Override
    public Event readSnapshot(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        for (ServiceWithInfo<ReadSnapshotInterceptor> snapshotReadInterceptor : readSnapshotInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), snapshotReadInterceptor.extensionKey())) {
                snapshot = snapshotReadInterceptor.service().readSnapshot(snapshot, extensionUnitOfWork);
            }
        }
        return snapshot;
    }

    @Override
    public Event readEvent(Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        for (ServiceWithInfo<ReadEventInterceptor> eventReadInterceptor : readEventInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), eventReadInterceptor.extensionKey())) {
                event = eventReadInterceptor.service().readEvent(event, extensionUnitOfWork);
            }
        }
        return event;
    }

    @Override
    public boolean noEventReadInterceptors() {
        ensureInitialized();
        return readEventInterceptors.isEmpty();
    }
}
