/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.interceptor.ExtensionEnabledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.transaction.Transactional;

/**
 * @author Marc Gathier
 */
@Component
public class ExtensionStatusManager implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(ExtensionStatusManager.class);
    private final ExtensionStatusRepository extensionStatusRepository;
    private final ExtensionConfigurationSerializer extensionConfigurationSerializer;
    private final ApplicationEventPublisher applicationEventPublisher;
    private boolean running;

    public ExtensionStatusManager(ExtensionStatusRepository extensionStatusRepository,
                                  ExtensionConfigurationSerializer extensionConfigurationSerializer,
                                  @Qualifier("localEventPublisher") ApplicationEventPublisher applicationEventPublisher) {
        this.extensionStatusRepository = extensionStatusRepository;
        this.extensionConfigurationSerializer = extensionConfigurationSerializer;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Transactional
    public ExtensionStatus updateStatus(String context, String extension, String version, String filename,
                                        boolean active) {
        ExtensionStatus extensionStatus = extensionStatusRepository.findByContextAndExtensionAndVersion(context,
                                                                                                        extension,
                                                                                                        version)
                                                                   .orElse(new ExtensionStatus(context,
                                                                                               extension,
                                                                                               version));
        if (active && !extensionStatus.isActive()) {
            extensionStatusRepository.findByContextAndExtensionAndActive(context, extension, true)
                                     .ifPresent(currentActive -> {
                                         currentActive.setActive(false);
                                         extensionStatusRepository.save(currentActive);
                                     });
        }

        extensionStatus.setActive(active);
        extensionStatus.setFilename(filename);
        extensionStatusRepository.save(extensionStatus);
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(context,
                                                                         new ExtensionKey(extension, version),
                                                                         extensionConfigurationSerializer
                                                                                 .deserialize(extensionStatus
                                                                                                      .getConfiguration()),
                                                                         active));
        return extensionStatus;
    }

    @Override
    public void start() {
        extensionStatusRepository.findAll().forEach(extensionStatus -> {
            if (extensionStatus.isActive()) {
                logger.warn("{}: Activate {}/{}",
                            extensionStatus.getContext(),
                            extensionStatus.getExtension(),
                            extensionStatus.getVersion());
                applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                                                 new ExtensionKey(extensionStatus
                                                                                                          .getExtension(),
                                                                                                  extensionStatus
                                                                                                          .getVersion()),
                                                                                 extensionConfigurationSerializer
                                                                                         .deserialize(extensionStatus
                                                                                                              .getConfiguration()),
                                                                                 true));
            } else {
                logger.warn("{}: Extension {}/{} not active",
                            extensionStatus.getContext(),
                            extensionStatus.getExtension(),
                            extensionStatus.getVersion());
            }
        });
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public int getPhase() {
        return 50;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public Optional<ExtensionStatus> getStatus(String context, String extension, String version) {
        return extensionStatusRepository.findByContextAndExtensionAndVersion(context, extension, version);
    }

    @Transactional
    public void update(ExtensionStatus updatedStatus) {
        extensionStatusRepository.save(updatedStatus);
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(updatedStatus.getContext(),
                                                                         new ExtensionKey(updatedStatus.getExtension(),
                                                                                          updatedStatus.getVersion()),
                                                                         extensionConfigurationSerializer
                                                                                 .deserialize(updatedStatus
                                                                                                      .getConfiguration()),
                                                                         updatedStatus.isActive()));
    }

    @Transactional
    public void remove(ExtensionStatus updatedStatus) {
        extensionStatusRepository.deleteById(updatedStatus.getId());
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(updatedStatus.getContext(),
                                                                         new ExtensionKey(updatedStatus.getExtension(),
                                                                                          updatedStatus.getVersion()),
                                                                         null,
                                                                         false));
    }

    public List<ExtensionStatus> findAllByContextIn(List<String> contextNames) {
        return extensionStatusRepository.findAllByContextIn(contextNames);
    }

    @Transactional
    public void updateConfiguration(String context, String extension, String version, String filename,
                                    Map<String, Map<String, Object>> properties) {
        ExtensionStatus extensionStatus = extensionStatusRepository.findByContextAndExtensionAndVersion(context,
                                                                                                        extension,
                                                                                                        version)
                                                                   .orElse(new ExtensionStatus(context,
                                                                                               extension,
                                                                                               version));
        extensionStatus.setFilename(filename);
        extensionStatus.setConfiguration(extensionConfigurationSerializer.serialize(properties));
        extensionStatusRepository.save(extensionStatus);
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(context,
                                                                         new ExtensionKey(extension, version),
                                                                         properties,
                                                                         extensionStatus.isActive()));
    }

    public void publishConfiguration(ExtensionKey extensionKey) {
        List<ExtensionStatus> contexts = extensionStatusRepository.findAllByExtensionAndVersion(extensionKey
                                                                                                        .getSymbolicName(),
                                                                                                extensionKey
                                                                                                        .getVersion());
        contexts.forEach(extensionStatus -> applicationEventPublisher
                .publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                        extensionKey,
                                                        extensionConfigurationSerializer
                                                                .deserialize(extensionStatus.getConfiguration()),
                                                        extensionStatus.isActive())));
    }

    @Transactional
    public void uninstall(ExtensionKey extensionKey) {
        List<ExtensionStatus> contexts = extensionStatusRepository.findAllByExtensionAndVersion(extensionKey
                                                                                                        .getSymbolicName(),
                                                                                                extensionKey
                                                                                                        .getVersion());
        contexts.forEach(extensionStatus -> {
            applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                                             extensionKey,
                                                                             null,
                                                                             false));
        });
        contexts.forEach(extensionStatus -> extensionStatusRepository.deleteById(extensionStatus.getId()));
    }

    @Transactional
    public void removeForContext(ExtensionKey extensionKey, String context) {
        Optional<ExtensionStatus> contexts =
                extensionStatusRepository.findByContextAndExtensionAndVersion(context,
                                                                              extensionKey.getSymbolicName(),
                                                                              extensionKey.getVersion());
        contexts.ifPresent(extensionStatus -> {
            applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                                             extensionKey,
                                                                             null,
                                                                             false));
            extensionStatusRepository.deleteById(extensionStatus.getId());
        });
    }

    @Transactional
    public void uninstallForContexts(List<String> contextNames) {
        List<ExtensionStatus> contexts = extensionStatusRepository.findAllByContextIn(contextNames);
        contexts.forEach(extensionStatus -> {
            applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                                             new ExtensionKey(
                                                                                     extensionStatus.getExtension(),
                                                                                     extensionStatus.getVersion()
                                                                             ),
                                                                             null,
                                                                             false));
        });
        contexts.forEach(extensionStatus -> extensionStatusRepository.deleteById(extensionStatus.getId()));
    }

    public Iterable<ExtensionInfo> listExtensions(
            Set<ExtensionKey> installedExtensions) {
        Map<ExtensionKey, ExtensionInfo> extensions = new HashMap<>();
        installedExtensions.forEach(e -> extensions.put(e, new ExtensionInfo(e.getSymbolicName(), e.getVersion())));
        extensionStatusRepository.findAll().forEach(extension -> {
            extensions.computeIfAbsent(new ExtensionKey(extension.getExtension(), extension.getVersion()), k ->
                    new ExtensionInfo(extension.getExtension(), extension.getVersion())
            ).addContextInfo(extension.getContext(), extension.isActive());
        });
        return extensions.values();
    }
}
