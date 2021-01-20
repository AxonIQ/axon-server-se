/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionServiceProvider;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.Ordered;
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
    private final ExtensionServiceProvider extensionServiceProvider;
    private final ExtensionContextFilter extensionContextFilter;
    private volatile boolean initialized;

    public DefaultEventInterceptors(
            ExtensionServiceProvider extensionServiceProvider,
            ExtensionContextFilter extensionContextFilter) {
        this.extensionServiceProvider = extensionServiceProvider;
        this.extensionContextFilter = extensionContextFilter;
        extensionServiceProvider.registerExtensionListener(serviceEvent -> {
            logger.debug("extension event {}", serviceEvent);
            initialized = false;
        });
    }


    private void ensureInitialized() {
        if (!initialized) {
            synchronized (extensionServiceProvider) {
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
        hooks.addAll(extensionServiceProvider.getServicesWithInfo(clazz));
        hooks.sort(Comparator.comparingInt(ServiceWithInfo::order));
        logger.debug("{} {}}", hooks.size(), clazz.getSimpleName());
    }

    @Override
    public Event appendEvent(
            Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        Event intercepted = event;
        for (ServiceWithInfo<AppendEventInterceptor> preCommitInterceptor : appendEventInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), preCommitInterceptor.extensionKey())) {
                intercepted = preCommitInterceptor.service().appendEvent(intercepted, extensionUnitOfWork);
            }
        }
        return mergeEvent(event, intercepted);
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
    public Event appendSnapshot(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        Event intercepted = snapshot;
        for (ServiceWithInfo<AppendSnapshotInterceptor> appendSnapshotInterceptor : appendSnapshotInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), appendSnapshotInterceptor.extensionKey())) {
                intercepted = appendSnapshotInterceptor.service().appendSnapshot(intercepted, extensionUnitOfWork);
            }
        }
        return mergeEvent(snapshot, intercepted);
    }

    @Override
    public boolean noReadInterceptors(String context) {
        ensureInitialized();
        return noSnapshotReadInterceptors(context) && noEventReadInterceptors(context);
    }

    @Override
    public Event readSnapshot(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        Event intercepted = snapshot;
        for (ServiceWithInfo<ReadSnapshotInterceptor> snapshotReadInterceptor : readSnapshotInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), snapshotReadInterceptor.extensionKey())) {
                intercepted = snapshotReadInterceptor.service().readSnapshot(intercepted, extensionUnitOfWork);
            }
        }
        return mergeEvent(snapshot, intercepted);
    }

    @Override
    public Event readEvent(Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        Event intercepted = event;
        for (ServiceWithInfo<ReadEventInterceptor> eventReadInterceptor : readEventInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), eventReadInterceptor.extensionKey())) {
                intercepted = eventReadInterceptor.service().readEvent(intercepted, extensionUnitOfWork);
            }
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
        ensureInitialized();
        return readEventInterceptors.stream().noneMatch(s -> extensionContextFilter.test(context, s.extensionKey()));
    }

    @Override
    public boolean noSnapshotReadInterceptors(String context) {
        ensureInitialized();
        return readSnapshotInterceptors.stream().noneMatch(s -> extensionContextFilter.test(context, s.extensionKey()));
    }
}
