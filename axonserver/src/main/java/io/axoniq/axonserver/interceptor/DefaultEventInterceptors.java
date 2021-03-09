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
import io.axoniq.axonserver.plugin.PluginUnitOfWork;
import io.axoniq.axonserver.plugin.RequestRejectedException;
import io.axoniq.axonserver.plugin.ServiceWithInfo;
import io.axoniq.axonserver.plugin.hook.PostCommitEventsHook;
import io.axoniq.axonserver.plugin.hook.PostCommitSnapshotHook;
import io.axoniq.axonserver.plugin.hook.PreCommitEventsHook;
import io.axoniq.axonserver.plugin.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.plugin.interceptor.AppendSnapshotInterceptor;
import io.axoniq.axonserver.plugin.interceptor.ReadEventInterceptor;
import io.axoniq.axonserver.plugin.interceptor.ReadSnapshotInterceptor;
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
    private final PluginContextFilter pluginContextFilter;
    private final InterceptorTimer interceptorTimer;


    public DefaultEventInterceptors(
            PluginContextFilter pluginContextFilter,
            MeterFactory meterFactory) {
        this.pluginContextFilter = pluginContextFilter;
        this.interceptorTimer = new InterceptorTimer(meterFactory);
    }

    @Override
    public Event appendEvent(
            Event event, PluginUnitOfWork unitOfWork) {
        List<ServiceWithInfo<AppendEventInterceptor>> interceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        AppendEventInterceptor.class,
                        unitOfWork
                                .context());
        if (interceptors.isEmpty()) {
            return event;
        }

        Event intercepted = interceptorTimer.time(unitOfWork.context(), "AppendEventInterceptor", () -> {
            Event e = event;
            for (ServiceWithInfo<AppendEventInterceptor> preCommitInterceptor : interceptors) {
                try {
                    e = preCommitInterceptor.service().appendEvent(e, unitOfWork);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         preCommitInterceptor.pluginKey()
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
                                PluginUnitOfWork unitOfWork) throws RequestRejectedException {
        List<ServiceWithInfo<PreCommitEventsHook>> servicesForContext = pluginContextFilter
                .getServicesWithInfoForContext(
                        PreCommitEventsHook.class,
                        unitOfWork.context());
        if (servicesForContext.isEmpty()) {
            return;
        }

        interceptorTimer.time(unitOfWork.context(), "PreCommitEventsHook", () -> {
            List<Event> immutableEvents = Collections.unmodifiableList(events);
            for (ServiceWithInfo<PreCommitEventsHook> preCommitEventsHook : servicesForContext) {
                try {
                    preCommitEventsHook.service().onPreCommitEvents(immutableEvents, unitOfWork);
                } catch (RequestRejectedException requestRejectedException) {
                    throw new MessagingPlatformException(ErrorCode.EVENT_REJECTED_BY_INTERCEPTOR,
                                                         unitOfWork.context() +
                                                                 ": Events rejected by the PreCommitEventsHook in "
                                                                 + preCommitEventsHook.pluginKey(),
                                                         requestRejectedException);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         unitOfWork.context() +
                                                                 ": Exception thrown by the PreCommitEventsHook in "
                                                                 + preCommitEventsHook.pluginKey(),
                                                         ex);
                }
            }
        });
    }

    @Override
    public void eventsPostCommit(List<Event> events, PluginUnitOfWork unitOfWork) {
        List<ServiceWithInfo<PostCommitEventsHook>> servicesForContext = pluginContextFilter
                .getServicesWithInfoForContext(
                        PostCommitEventsHook.class,
                        unitOfWork.context());
        if (servicesForContext.isEmpty()) {
            return;
        }

        interceptorTimer.time(unitOfWork.context(), "PostCommitEventsHook", () -> {
            List<Event> immutableList = Collections.unmodifiableList(events);
            for (ServiceWithInfo<PostCommitEventsHook> postCommitEventsHook : servicesForContext) {
                try {
                    postCommitEventsHook.service().onPostCommitEvent(immutableList, unitOfWork);
                } catch (Exception ex) {
                    logger.warn("{} : Exception thrown by the PostCommitEventsHook in {}",
                                unitOfWork.context(), postCommitEventsHook.pluginKey(), ex);
                }
            }
        });
    }

    @Override
    public void snapshotPostCommit(Event snapshot, PluginUnitOfWork unitOfWork) {
        List<ServiceWithInfo<PostCommitSnapshotHook>> interceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        PostCommitSnapshotHook.class,
                        unitOfWork.context());
        if (interceptors.isEmpty()) {
            return;
        }

        interceptorTimer.time(unitOfWork.context(), "PostCommitEventsHook", () -> {
            for (ServiceWithInfo<PostCommitSnapshotHook> postCommitSnapshotHook : interceptors) {
                try {
                    postCommitSnapshotHook.service().onPostCommitSnapshot(snapshot, unitOfWork);
                } catch (Exception ex) {
                    logger.warn("{} : Exception thrown by the PostCommitSnapshotHook in {}",
                                unitOfWork.context(), postCommitSnapshotHook.pluginKey(), ex);
                }
            }
        });
    }

    @Override
    public Event appendSnapshot(Event snapshot, PluginUnitOfWork unitOfWork)
            throws RequestRejectedException {
        List<ServiceWithInfo<AppendSnapshotInterceptor>> interceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        AppendSnapshotInterceptor.class,
                        unitOfWork.context());
        if (interceptors.isEmpty()) {
            return snapshot;
        }

        Event intercepted = interceptorTimer.time(unitOfWork.context(), "AppendSnapshotInterceptor", () -> {
            Event s = snapshot;
            for (ServiceWithInfo<AppendSnapshotInterceptor> appendSnapshotInterceptor : interceptors) {
                try {
                    s = appendSnapshotInterceptor.service().appendSnapshot(s, unitOfWork);
                } catch (RequestRejectedException requestRejectedException) {
                    throw new MessagingPlatformException(ErrorCode.EVENT_REJECTED_BY_INTERCEPTOR,
                                                         unitOfWork.context() +
                                                                 ": Snapshot rejected by the AppendSnapshotInterceptor in "
                                                                 + appendSnapshotInterceptor.pluginKey(),
                                                         requestRejectedException);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         unitOfWork.context() +
                                                                 ": Exception thrown by the AppendSnapshotInterceptor in "
                                                                 + appendSnapshotInterceptor.pluginKey(),
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
    public Event readSnapshot(Event snapshot, PluginUnitOfWork unitOfWork) {
        Event intercepted = interceptorTimer.time(unitOfWork.context(), "ReadSnapshotInterceptor", () -> {
            Event s = snapshot;

            for (ServiceWithInfo<ReadSnapshotInterceptor> snapshotReadInterceptor : pluginContextFilter
                    .getServicesWithInfoForContext(
                            ReadSnapshotInterceptor.class,
                            unitOfWork.context()
                    )) {
                try {
                    s = snapshotReadInterceptor.service().readSnapshot(s, unitOfWork);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         unitOfWork.context() +
                                                                 ": Exception thrown by the ReadSnapshotInterceptor in "
                                                                 + snapshotReadInterceptor.pluginKey(),
                                                         ex);
                }
            }
            return s;
        });
        return mergeEvent(snapshot, intercepted);
    }

    @Override
    public Event readEvent(Event event, PluginUnitOfWork unitOfWork) {
        Event intercepted = interceptorTimer.time(unitOfWork.context(), "ReadEventInterceptor", () -> {
            Event e = event;
            for (ServiceWithInfo<ReadEventInterceptor> eventReadInterceptor : pluginContextFilter
                    .getServicesWithInfoForContext(
                            ReadEventInterceptor.class,
                            unitOfWork.context()
                    )) {
                try {
                    e = eventReadInterceptor.service().readEvent(e, unitOfWork);
                } catch (Exception ex) {
                    throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                         unitOfWork.context() +
                                                                 ": Exception thrown by the ReadEventInterceptor in "
                                                                 + eventReadInterceptor.pluginKey(),
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
        return pluginContextFilter.getServicesForContext(ReadEventInterceptor.class, context).isEmpty();
    }

    @Override
    public boolean noSnapshotReadInterceptors(String context) {
        return pluginContextFilter.getServicesForContext(ReadSnapshotInterceptor.class, context).isEmpty();
    }
}
