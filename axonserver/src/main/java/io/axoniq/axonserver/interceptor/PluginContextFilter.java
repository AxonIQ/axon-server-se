/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.plugin.PluginServiceProvider;
import io.axoniq.axonserver.plugin.Ordered;
import io.axoniq.axonserver.plugin.ServiceWithInfo;
import io.axoniq.axonserver.plugin.hook.PostCommitEventsHook;
import io.axoniq.axonserver.plugin.hook.PostCommitSnapshotHook;
import io.axoniq.axonserver.plugin.hook.PreCommitEventsHook;
import io.axoniq.axonserver.plugin.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.plugin.interceptor.AppendSnapshotInterceptor;
import io.axoniq.axonserver.plugin.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.plugin.interceptor.QueryRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.QueryResponseInterceptor;
import io.axoniq.axonserver.plugin.interceptor.ReadEventInterceptor;
import io.axoniq.axonserver.plugin.interceptor.ReadSnapshotInterceptor;
import io.axoniq.axonserver.plugin.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.SubscriptionQueryResponseInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Checks if an plugin is active for a specific context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class PluginContextFilter {

    @SuppressWarnings("unchecked")
    private final Class<? extends Ordered>[] interceptorClasses = new Class[]{
            AppendEventInterceptor.class,
            PreCommitEventsHook.class,
            PostCommitEventsHook.class,
            AppendSnapshotInterceptor.class,
            PostCommitSnapshotHook.class,
            ReadEventInterceptor.class,
            ReadSnapshotInterceptor.class,
            CommandRequestInterceptor.class,
            CommandResponseInterceptor.class,
            QueryRequestInterceptor.class,
            QueryResponseInterceptor.class,
            SubscriptionQueryRequestInterceptor.class,
            SubscriptionQueryResponseInterceptor.class
    };
    private final Logger logger = LoggerFactory.getLogger(PluginContextFilter.class);
    private final Map<String, Map<String, String>> enabledPluginsPerContext = new ConcurrentHashMap<>();
    private final PluginServiceProvider pluginServiceProvider;
    private final Map<Class<? extends Ordered>, List<ServiceWithInfo<Ordered>>> serviceMap = new HashMap<>();
    private final boolean enabled;
    private volatile boolean initialized;

    @Autowired
    public PluginContextFilter(PluginServiceProvider pluginServiceProvider,
                               MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this(pluginServiceProvider, messagingPlatformConfiguration.isPluginsEnabled());
    }

    public PluginContextFilter(PluginServiceProvider pluginServiceProvider,
                               boolean pluginEnabled) {
        this.pluginServiceProvider = pluginServiceProvider;
        this.enabled = pluginEnabled;
        pluginServiceProvider.registerPluginListener((plugin, status) -> initialized = false);
    }


    private void ensureInitialized() {
        if (enabled && !initialized) {
            synchronized (pluginServiceProvider) {
                if (initialized) {
                    return;
                }

                for (Class<? extends Ordered> interceptorClass : interceptorClasses) {
                    //noinspection unchecked
                    serviceMap.put(interceptorClass, initHooks((Class<Ordered>) interceptorClass));
                }

                initialized = true;
            }
        }
    }

    private <T extends Ordered> List<ServiceWithInfo<T>> initHooks(Class<T> interceptorClass) {
        List<ServiceWithInfo<T>> hooks = new ArrayList<>(pluginServiceProvider
                                                                 .getServicesWithInfo(interceptorClass));
        hooks.sort(ServiceWithInfo::compareTo);
        logger.debug("{} {}}", hooks.size(), interceptorClass.getSimpleName());
        return hooks;
    }

    public <T extends Ordered> List<ServiceWithInfo<T>> getServicesWithInfoForContext(Class<T> interceptorClass,
                                                                                      String context) {
        if (!enabled) {
            return Collections.emptyList();
        }
        ensureInitialized();
        List<ServiceWithInfo<T>> interceptors = new ArrayList<>();
        Map<String, String> enabledPlugins = enabledPluginsPerContext.getOrDefault(context,
                                                                                   Collections.emptyMap());
        serviceMap.get(interceptorClass).forEach(service -> {
            if (service.pluginKey().getVersion().equals(enabledPlugins
                                                                .get(service.pluginKey().getSymbolicName()))) {
                //noinspection unchecked
                interceptors.add((ServiceWithInfo<T>) service);
            }
        });
        return interceptors;
    }

    public <T extends Ordered> List<T> getServicesForContext(Class<T> interceptorClass, String context) {
        if (!enabled) {
            return Collections.emptyList();
        }
        ensureInitialized();
        List<T> interceptors = new ArrayList<>();
        Map<String, String> enabledPlugins = enabledPluginsPerContext.getOrDefault(context,
                                                                                   Collections.emptyMap());
        serviceMap.get(interceptorClass).forEach(service -> {
            if (service.pluginKey().getVersion().equals(enabledPlugins
                                                                .get(service.pluginKey().getSymbolicName()))) {
                //noinspection unchecked
                interceptors.add((T) service.service());
            }
        });
        return interceptors;
    }

    /**
     * Handles {@link PluginEnabledEvent} events, published when an plugin becomes active or is
     * de-activated for a specific context.
     *
     * @param pluginEnabledEvent the event
     */
    @EventListener
    @Order(100)
    public void on(PluginEnabledEvent pluginEnabledEvent) {
        if (pluginEnabledEvent.enabled()) {
            String oldVersion = enabledPluginsPerContext.computeIfAbsent(pluginEnabledEvent.context(),
                                                                         c -> new ConcurrentHashMap<>())
                                                        .put(pluginEnabledEvent.plugin().getSymbolicName(),
                                                             pluginEnabledEvent.plugin().getVersion());
            if (oldVersion == null || !pluginEnabledEvent.plugin().getVersion().equals(oldVersion)) {
                if (logger.isInfoEnabled()) {
                    logger.info("{}: Plugin {} activated", pluginEnabledEvent.context(), pluginEnabledEvent.plugin());
                }
            }
        } else {
            String oldVersion = enabledPluginsPerContext.getOrDefault(pluginEnabledEvent.context(),
                                                                      Collections.emptyMap())
                                                        .get(pluginEnabledEvent.plugin().getSymbolicName());
            if (oldVersion != null && pluginEnabledEvent.plugin().getVersion().equals(oldVersion)) {
                enabledPluginsPerContext.get(pluginEnabledEvent.context())
                                        .remove(pluginEnabledEvent.plugin().getSymbolicName());
                if (logger.isInfoEnabled()) {
                    logger.info("{}: Plugin {} deactivated",
                                pluginEnabledEvent.context(),
                                pluginEnabledEvent.plugin());
                }
            }
        }
    }
}
