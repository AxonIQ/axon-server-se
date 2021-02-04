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
import io.axoniq.axonserver.extensions.ExtensionServiceProvider;
import io.axoniq.axonserver.extensions.Ordered;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.hook.PostCommitEventsHook;
import io.axoniq.axonserver.extensions.hook.PostCommitSnapshotHook;
import io.axoniq.axonserver.extensions.hook.PreCommitEventsHook;
import io.axoniq.axonserver.extensions.interceptor.AppendEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.AppendSnapshotInterceptor;
import io.axoniq.axonserver.extensions.interceptor.CommandRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.CommandResponseInterceptor;
import io.axoniq.axonserver.extensions.interceptor.QueryRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.QueryResponseInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadEventInterceptor;
import io.axoniq.axonserver.extensions.interceptor.ReadSnapshotInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryResponseInterceptor;
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
 * Checks if an extension is active for a specific context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class ExtensionContextFilter {

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
    private final Logger logger = LoggerFactory.getLogger(ExtensionContextFilter.class);
    private final Map<String, Map<String, String>> enabledExtensionsPerContext = new ConcurrentHashMap<>();
    private final ExtensionServiceProvider extensionServiceProvider;
    private final Map<Class<? extends Ordered>, List<ServiceWithInfo<Ordered>>> serviceMap = new HashMap<>();
    private final boolean enabled;
    private volatile boolean initialized;

    @Autowired
    public ExtensionContextFilter(ExtensionServiceProvider extensionServiceProvider,
                                  MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this(extensionServiceProvider, messagingPlatformConfiguration.isExtensionsEnabled());
    }

    public ExtensionContextFilter(ExtensionServiceProvider extensionServiceProvider,
                                  boolean extensionEnabled) {
        this.extensionServiceProvider = extensionServiceProvider;
        this.enabled = extensionEnabled;
        extensionServiceProvider.registerExtensionListener((extension, status) -> initialized = false);
    }


    private void ensureInitialized() {
        if (enabled && !initialized) {
            synchronized (extensionServiceProvider) {
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
        List<ServiceWithInfo<T>> hooks = new ArrayList<>(extensionServiceProvider
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
        Map<String, String> enabledExtensions = enabledExtensionsPerContext.getOrDefault(context,
                                                                                         Collections.emptyMap());
        serviceMap.get(interceptorClass).forEach(service -> {
            if (service.extensionKey().getVersion().equals(enabledExtensions
                                                                   .get(service.extensionKey().getSymbolicName()))) {
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
        Map<String, String> enabledExtensions = enabledExtensionsPerContext.getOrDefault(context,
                                                                                         Collections.emptyMap());
        serviceMap.get(interceptorClass).forEach(service -> {
            if (service.extensionKey().getVersion().equals(enabledExtensions
                                                                   .get(service.extensionKey().getSymbolicName()))) {
                //noinspection unchecked
                interceptors.add((T) service.service());
            }
        });
        return interceptors;
    }

    /**
     * Handles {@link ExtensionEnabledEvent} events, published when an extension becomes active or is
     * de-activated for a specific context.
     *
     * @param extensionEnabled the event
     */
    @EventListener
    @Order(100)
    public void on(ExtensionEnabledEvent extensionEnabled) {
        if (extensionEnabled.enabled()) {
            String oldVersion = enabledExtensionsPerContext.computeIfAbsent(extensionEnabled.context(),
                                                                            c -> new ConcurrentHashMap<>())
                                                           .put(extensionEnabled.extension().getSymbolicName(),
                                                                extensionEnabled.extension().getVersion());
            if (oldVersion == null || !extensionEnabled.extension().getVersion().equals(oldVersion)) {
                if (logger.isInfoEnabled()) {
                    logger.info("{}: Extension {} activated", extensionEnabled.context(), extensionEnabled.extension());
                }
            }
        } else {
            String oldVersion = enabledExtensionsPerContext.getOrDefault(extensionEnabled.context(),
                                                                         Collections.emptyMap())
                                                           .get(extensionEnabled.extension().getSymbolicName());
            if (oldVersion != null && extensionEnabled.extension().getVersion().equals(oldVersion)) {
                enabledExtensionsPerContext.get(extensionEnabled.context())
                                           .remove(extensionEnabled.extension().getSymbolicName());
                if (logger.isInfoEnabled()) {
                    logger.info("{}: Extension {} deactivated",
                                extensionEnabled.context(),
                                extensionEnabled.extension());
                }
            }
        }
    }
}
