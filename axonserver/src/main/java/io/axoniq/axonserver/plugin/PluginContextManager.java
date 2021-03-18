/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.interceptor.PluginEnabledEvent;
import io.axoniq.axonserver.interceptor.PluginRemovedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.transaction.Transactional;

/**
 * Manages the status and configuration of a plugin per context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class PluginContextManager {

    private final Logger logger = LoggerFactory.getLogger(PluginContextManager.class);
    private final PluginStatusRepository pluginStatusRepository;
    private final PluginPackageRepository packageRepository;
    private final PluginConfigurationSerializer pluginConfigurationSerializer;
    private final OsgiController osgiController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public PluginContextManager(PluginStatusRepository pluginStatusRepository,
                                PluginPackageRepository packageRepository,
                                PluginConfigurationSerializer pluginConfigurationSerializer,
                                OsgiController osgiController,
                                @Qualifier("localEventPublisher") ApplicationEventPublisher applicationEventPublisher) {
        this.pluginStatusRepository = pluginStatusRepository;
        this.packageRepository = packageRepository;
        this.pluginConfigurationSerializer = pluginConfigurationSerializer;
        this.osgiController = osgiController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Transactional
    public ContextPluginStatus updateStatus(String context, String pluginName, String version,
                                            boolean active) {
        PluginPackage pack = packageRepository.findByNameAndVersion(pluginName, version)
                                              .orElseThrow(() -> new PluginNotFoundException(pluginName, version));

        ContextPluginStatus pluginStatus = pluginStatusRepository.findByContextAndPlugin(context, pack)
                                                                 .orElse(new ContextPluginStatus(context,
                                                                                                 pack));
        if (active && !pluginStatus.isActive()) {
            pluginStatusRepository.findByContextAndPlugin_NameAndActive(context, pluginName, true)
                                  .ifPresent(currentActive -> {
                                      currentActive.setActive(false);
                                      pluginStatusRepository.save(currentActive);
                                      applicationEventPublisher.publishEvent(new PluginEnabledEvent(context,
                                                                                                    currentActive
                                                                                                            .getPluginKey(),
                                                                                                    pluginConfigurationSerializer
                                                                                                            .deserialize(
                                                                                                                    currentActive
                                                                                                                            .getConfiguration()),
                                                                                                    false));
                                  });
        }

        pluginStatus.setActive(active);
        pluginStatusRepository.save(pluginStatus);
        applicationEventPublisher.publishEvent(new PluginEnabledEvent(context,
                                                                      new PluginKey(pluginName, version),
                                                                      pluginConfigurationSerializer
                                                                              .deserialize(pluginStatus
                                                                                                   .getConfiguration()),
                                                                      active));
        return pluginStatus;
    }

    public void start() {
        pluginStatusRepository.findAll().forEach(pluginStatus -> {
            if (pluginStatus.isActive()) {
                logger.info("{}: Activate {}/{}",
                            pluginStatus.getContext(),
                            pluginStatus.getPlugin().getName(),
                            pluginStatus.getPlugin().getVersion());
                applicationEventPublisher.publishEvent(new PluginEnabledEvent(pluginStatus.getContext(),
                                                                              pluginStatus.getPluginKey(),
                                                                              pluginConfigurationSerializer
                                                                                      .deserialize(pluginStatus
                                                                                                           .getConfiguration()),
                                                                              true));
            } else {
                logger.info("{}: Plugin {} not active",
                            pluginStatus.getContext(),
                            pluginStatus.getPluginKey());
            }
        });
    }


    public Optional<ContextPluginStatus> getStatus(String context, String pluginName, String version) {
        PluginPackage pack = packageRepository.findByNameAndVersion(pluginName, version)
                                              .orElseThrow(() -> new PluginNotFoundException(pluginName, version));
        return pluginStatusRepository.findByContextAndPlugin(context, pack);
    }

    @Transactional
    public void update(ContextPluginStatus updatedStatus) {
        pluginStatusRepository.save(updatedStatus);
        applicationEventPublisher.publishEvent(new PluginEnabledEvent(updatedStatus.getContext(),
                                                                      updatedStatus.getPluginKey(),
                                                                      pluginConfigurationSerializer
                                                                              .deserialize(updatedStatus
                                                                                                   .getConfiguration()),
                                                                      updatedStatus.isActive()));
    }

    @Transactional
    public void remove(ContextPluginStatus updatedStatus) {
        pluginStatusRepository.deleteById(updatedStatus.getId());
        applicationEventPublisher.publishEvent(new PluginRemovedEvent(updatedStatus.getContext(),
                                                                      updatedStatus.getPluginKey()));
    }

    public List<ContextPluginStatus> findAllByContextIn(List<String> contextNames) {
        return pluginStatusRepository.findAllByContextIn(contextNames);
    }

    @Transactional
    public void updateConfiguration(String context, String pluginName, String version,
                                    Map<String, Map<String, Object>> properties) {
        PluginPackage pack = packageRepository.findByNameAndVersion(pluginName, version)
                                              .orElseThrow(() -> new PluginNotFoundException(pluginName, version));
        ContextPluginStatus pluginStatus = pluginStatusRepository.findByContextAndPlugin(context,
                                                                                         pack)
                                                                 .orElse(new ContextPluginStatus(context,
                                                                                                 pack));
        pluginStatus.setConfiguration(pluginConfigurationSerializer.serialize(properties));
        pluginStatusRepository.save(pluginStatus);
        applicationEventPublisher.publishEvent(new PluginEnabledEvent(context,
                                                                      new PluginKey(pluginName, version),
                                                                      properties,
                                                                      pluginStatus.isActive()));
    }

    public void publishConfiguration(PluginPackage pack) {
        List<ContextPluginStatus> contexts = pluginStatusRepository.findAllByPlugin(pack);
        contexts.forEach(pluginStatus -> applicationEventPublisher
                .publishEvent(new PluginEnabledEvent(pluginStatus.getContext(),
                                                     pack.getKey(),
                                                     pluginConfigurationSerializer
                                                             .deserialize(pluginStatus.getConfiguration()),
                                                     pluginStatus.isActive())));
    }

    @Transactional
    public void uninstall(PluginKey pluginKey) {
        Optional<PluginPackage> pack = packageRepository.findByNameAndVersion(pluginKey.getSymbolicName(),
                                                                              pluginKey.getVersion());
        if (pack.isPresent()) {
            List<ContextPluginStatus> contexts = pluginStatusRepository.findAllByPlugin(pack.get());
            contexts.forEach(pluginStatus -> applicationEventPublisher
                    .publishEvent(new PluginEnabledEvent(pluginStatus.getContext(),
                                                         pluginKey,
                                                         null,
                                                         false)));
            contexts.forEach(pluginStatus -> pluginStatusRepository.deleteById(pluginStatus.getId()));
        }
    }

    @Transactional
    public void removeForContext(PluginKey pluginKey, String context) {
        Optional<PluginPackage> pack = packageRepository.findByNameAndVersion(pluginKey.getSymbolicName(),
                                                                              pluginKey.getVersion());
        if (!pack.isPresent()) {
            return;
        }

        Optional<ContextPluginStatus> contexts =
                pluginStatusRepository.findByContextAndPlugin(context, pack.get());
        contexts.ifPresent(pluginStatus -> {
            applicationEventPublisher.publishEvent(new PluginEnabledEvent(pluginStatus.getContext(),
                                                                          pluginKey,
                                                                          null,
                                                                          false));
            pluginStatusRepository.deleteById(pluginStatus.getId());
        });
    }

    @Transactional
    public void uninstallForContexts(List<String> contextNames) {
        List<ContextPluginStatus> contexts = pluginStatusRepository.findAllByContextIn(contextNames);
        contexts.forEach(pluginStatus -> {
            pluginStatusRepository.deleteById(pluginStatus.getId());
            applicationEventPublisher.publishEvent(new PluginEnabledEvent(pluginStatus.getContext(),
                                                                          pluginStatus.getPluginKey(),
                                                                          null,
                                                                          false));
        });
    }

    public Iterable<PluginInfo> listPlugins() {
        Map<PluginKey, PluginInfo> pluginInfoHashMap = new HashMap<>();
        packageRepository.findAll()
                         .forEach(pack -> pluginInfoHashMap.put(pack.getKey(),
                                                                new PluginInfo(pack.getName(),
                                                                               pack.getVersion(),
                                                                               pack.getFilename(),
                                                                               osgiController
                                                                                       .getStatus(pack.getKey()))));
        pluginStatusRepository.findAll()
                              .forEach(pluginStatus -> pluginInfoHashMap.get(pluginStatus.getPluginKey())
                                                                        .addContextInfo(pluginStatus.getContext(),
                                                                                        pluginStatus.isActive()));
        return pluginInfoHashMap.values();
    }

    public List<ContextPluginStatus> findAll(PluginKey pluginKey) {
        return packageRepository.findByNameAndVersion(pluginKey.getSymbolicName(), pluginKey.getVersion())
                                .map(pluginStatusRepository::findAllByPlugin)
                                .orElse(Collections.emptyList());
    }
}
