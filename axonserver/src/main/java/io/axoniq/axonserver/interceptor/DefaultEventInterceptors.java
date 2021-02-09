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
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.RequestRejectedException;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.hook.PostCommitEventsHook;
import io.axoniq.axonserver.extensions.hook.PostCommitSnapshotHook;
import io.axoniq.axonserver.extensions.hook.PreCommitEventsHook;
import io.axoniq.axonserver.extensions.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.AppendSnapshotInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadSnapshotInterceptor;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.metric.MeterFactory;
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
    private final InterceptorTimer interceptorTimer;


    public DefaultEventInterceptors(
            ExtensionContextFilter extensionContextFilter,
            MeterFactory meterFactory) {
        this.extensionContextFilter = extensionContextFilter;
        this.interceptorTimer = new InterceptorTimer(meterFactory);
    }

    @Override
    public Event appendEvent(
            Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        List<ServiceWithInfo<AppendEventInterceptor>> interceptors = extensionContextFilter
                .getServicesWithInfoForContext(
                        AppendEventInterceptor.class,
                        extensionUnitOfWork
                                .context());
        if (interceptors.isEmpty()) {
            return event;
        }

        Event intercepted = interceptorTimer.time(extensionUnitOfWork.context(), "AppendEventInterceptor", () -> {
            Event e = event;
            for (ServiceWithInfo<AppendEventInterceptor> preCommitInterceptor : interceptors) {
                try {
                    e = preCommitInterceptor.service().appendEvent(e, extensionUnitOfWork);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         preCommitInterceptor.extensionKey()
                                                                 + ": Error in AppendEventInterceptor",
                                                         ex);
                }
            }
            return e;
        });

        return mergeEvent(event, intercepted);
    }


    @Override
    public void eventsPreCommit(List<Event> events,
                                ExtensionUnitOfWork extensionUnitOfWork) throws RequestRejectedException {
        List<ServiceWithInfo<PreCommitEventsHook>> servicesForContext = extensionContextFilter
                .getServicesWithInfoForContext(
                        PreCommitEventsHook.class,
                        extensionUnitOfWork.context());
        if (servicesForContext.isEmpty()) {
            return;
        }

        interceptorTimer.time(extensionUnitOfWork.context(), "PreCommitEventsHook", () -> {
            List<Event> immutableEvents = Collections.unmodifiableList(events);
            for (ServiceWithInfo<PreCommitEventsHook> preCommitEventsHook : servicesForContext) {
                try {
                    preCommitEventsHook.service().onPreCommitEvents(immutableEvents, extensionUnitOfWork);
                } catch (RequestRejectedException requestRejectedException) {
                    throw new MessagingPlatformException(ErrorCode.EVENT_REJECTED_BY_INTERCEPTOR,
                                                         extensionUnitOfWork.context() +
                                                                 ": Events rejected by the PreCommitEventsHook in "
                                                                 + preCommitEventsHook.extensionKey(),
                                                         requestRejectedException);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         extensionUnitOfWork.context() +
                                                                 ": Exception thrown by the PreCommitEventsHook in "
                                                                 + preCommitEventsHook.extensionKey(),
                                                         ex);
                }
            }
        });
    }

    @Override
    public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork extensionUnitOfWork) {
        List<ServiceWithInfo<PostCommitEventsHook>> servicesForContext = extensionContextFilter
                .getServicesWithInfoForContext(
                        PostCommitEventsHook.class,
                        extensionUnitOfWork.context());
        if (servicesForContext.isEmpty()) {
            return;
        }

        interceptorTimer.time(extensionUnitOfWork.context(), "PostCommitEventsHook", () -> {
            List<Event> immutableList = Collections.unmodifiableList(events);
            for (ServiceWithInfo<PostCommitEventsHook> postCommitEventsHook : servicesForContext) {
                try {
                    postCommitEventsHook.service().onPostCommitEvent(immutableList, extensionUnitOfWork);
                } catch (Exception ex) {
                    logger.warn("{} : Exception thrown by the PostCommitEventsHook in {}",
                                extensionUnitOfWork.context(), postCommitEventsHook.extensionKey(), ex);
                }
            }
        });
    }

    @Override
    public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        List<ServiceWithInfo<PostCommitSnapshotHook>> interceptors = extensionContextFilter
                .getServicesWithInfoForContext(
                        PostCommitSnapshotHook.class,
                        extensionUnitOfWork.context());
        if (interceptors.isEmpty()) {
            return;
        }

        interceptorTimer.time(extensionUnitOfWork.context(), "PostCommitEventsHook", () -> {
            for (ServiceWithInfo<PostCommitSnapshotHook> postCommitSnapshotHook : interceptors) {
                try {
                    postCommitSnapshotHook.service().onPostCommitSnapshot(snapshot, extensionUnitOfWork);
                } catch (Exception ex) {
                    logger.warn("{} : Exception thrown by the PostCommitSnapshotHook in {}",
                                extensionUnitOfWork.context(), postCommitSnapshotHook.extensionKey(), ex);
                }
            }
        });
    }

    @Override
    public Event appendSnapshot(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork)
            throws RequestRejectedException {
        List<ServiceWithInfo<AppendSnapshotInterceptor>> interceptors = extensionContextFilter
                .getServicesWithInfoForContext(
                        AppendSnapshotInterceptor.class,
                        extensionUnitOfWork.context());
        if (interceptors.isEmpty()) {
            return snapshot;
        }

        Event intercepted = interceptorTimer.time(extensionUnitOfWork.context(), "AppendSnapshotInterceptor", () -> {
            Event s = snapshot;
            for (ServiceWithInfo<AppendSnapshotInterceptor> appendSnapshotInterceptor : interceptors) {
                try {
                    s = appendSnapshotInterceptor.service().appendSnapshot(s, extensionUnitOfWork);
                } catch (RequestRejectedException requestRejectedException) {
                    throw new MessagingPlatformException(ErrorCode.EVENT_REJECTED_BY_INTERCEPTOR,
                                                         extensionUnitOfWork.context() +
                                                                 ": Snapshot rejected by the AppendSnapshotInterceptor in "
                                                                 + appendSnapshotInterceptor.extensionKey(),
                                                         requestRejectedException);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         extensionUnitOfWork.context() +
                                                                 ": Exception thrown by the AppendSnapshotInterceptor in "
                                                                 + appendSnapshotInterceptor.extensionKey(),
                                                         ex);
                }
            }
            return s;
        });

        return mergeEvent(snapshot, intercepted);
    }

    @Override
    public boolean noReadInterceptors(String context) {
        return noSnapshotReadInterceptors(context) && noEventReadInterceptors(context);
    }

    @Override
    public Event readSnapshot(Event snapshot, ExtensionUnitOfWork extensionUnitOfWork) {
        Event intercepted = interceptorTimer.time(extensionUnitOfWork.context(), "ReadSnapshotInterceptor", () -> {
            Event s = snapshot;

            for (ServiceWithInfo<ReadSnapshotInterceptor> snapshotReadInterceptor : extensionContextFilter
                    .getServicesWithInfoForContext(
                            ReadSnapshotInterceptor.class,
                            extensionUnitOfWork.context()
                    )) {
                try {
                    s = snapshotReadInterceptor.service().readSnapshot(s, extensionUnitOfWork);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         extensionUnitOfWork.context() +
                                                                 ": Exception thrown by the ReadSnapshotInterceptor in "
                                                                 + snapshotReadInterceptor.extensionKey(),
                                                         ex);
                }
            }
            return s;
        });
        return mergeEvent(snapshot, intercepted);
    }

    @Override
    public Event readEvent(Event event, ExtensionUnitOfWork extensionUnitOfWork) {
        Event intercepted = interceptorTimer.time(extensionUnitOfWork.context(), "ReadEventInterceptor", () -> {
            Event e = event;
            for (ServiceWithInfo<ReadEventInterceptor> eventReadInterceptor : extensionContextFilter
                    .getServicesWithInfoForContext(
                            ReadEventInterceptor.class,
                            extensionUnitOfWork.context()
                    )) {
                try {
                    e = eventReadInterceptor.service().readEvent(e, extensionUnitOfWork);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         extensionUnitOfWork.context() +
                                                                 ": Exception thrown by the ReadEventInterceptor in "
                                                                 + eventReadInterceptor.extensionKey(),
                                                         ex);
                }
            }
            return e;
        });
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
