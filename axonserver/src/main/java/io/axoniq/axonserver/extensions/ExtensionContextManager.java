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
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.transaction.Transactional;

/**
 * Manages the status and configuration of an extension per context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class ExtensionContextManager {

    private final Logger logger = LoggerFactory.getLogger(ExtensionContextManager.class);
    private final ExtensionStatusRepository extensionStatusRepository;
    private final ExtensionPackageRepository packageRepository;
    private final ExtensionConfigurationSerializer extensionConfigurationSerializer;
    private final OsgiController osgiController;
    private final ApplicationEventPublisher applicationEventPublisher;

    public ExtensionContextManager(ExtensionStatusRepository extensionStatusRepository,
                                   ExtensionPackageRepository packageRepository,
                                   ExtensionConfigurationSerializer extensionConfigurationSerializer,
                                   OsgiController osgiController,
                                   @Qualifier("localEventPublisher") ApplicationEventPublisher applicationEventPublisher) {
        this.extensionStatusRepository = extensionStatusRepository;
        this.packageRepository = packageRepository;
        this.extensionConfigurationSerializer = extensionConfigurationSerializer;
        this.osgiController = osgiController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Transactional
    public ExtensionStatus updateStatus(String context, String extension, String version,
                                        boolean active) {
        ExtensionPackage pack = packageRepository.findByExtensionAndVersion(extension, version)
                                                 .orElseThrow(() -> new ExtensionNotFoundException(extension, version));

        ExtensionStatus extensionStatus = extensionStatusRepository.findByContextAndExtension(context, pack)
                                                                   .orElse(new ExtensionStatus(context,
                                                                                               pack));
        if (active && !extensionStatus.isActive()) {
            extensionStatusRepository.findByContextAndExtension_ExtensionAndActive(context, extension, true)
                                     .ifPresent(currentActive -> {
                                         currentActive.setActive(false);
                                         extensionStatusRepository.save(currentActive);
                                         applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(context,
                                                                                                          currentActive
                                                                                                                  .getExtensionKey(),
                                                                                                          extensionConfigurationSerializer
                                                                                                                  .deserialize(
                                                                                                                          currentActive
                                                                                                                                  .getConfiguration()),
                                                                                                          false));
                                     });
        }

        extensionStatus.setActive(active);
        extensionStatusRepository.save(extensionStatus);
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(context,
                                                                         new ExtensionKey(extension, version),
                                                                         extensionConfigurationSerializer
                                                                                 .deserialize(extensionStatus
                                                                                                      .getConfiguration()),
                                                                         active));
        return extensionStatus;
    }

    public void start() {
        packageRepository.findAll().forEach(extensionPackage -> {


        });
        extensionStatusRepository.findAll().forEach(extensionStatus -> {
            if (extensionStatus.isActive()) {
                logger.info("{}: Activate {}/{}",
                            extensionStatus.getContext(),
                            extensionStatus.getExtension().getExtension(),
                            extensionStatus.getExtension().getVersion());
                applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                                                 extensionStatus.getExtensionKey(),
                                                                                 extensionConfigurationSerializer
                                                                                         .deserialize(extensionStatus
                                                                                                              .getConfiguration()),
                                                                                 true));
            } else {
                logger.info("{}: Extension {} not active",
                            extensionStatus.getContext(),
                            extensionStatus.getExtensionKey());
            }
        });
    }


    public Optional<ExtensionStatus> getStatus(String context, String extension, String version) {
        ExtensionPackage pack = packageRepository.findByExtensionAndVersion(extension, version)
                                                 .orElseThrow(() -> new ExtensionNotFoundException(extension, version));
        return extensionStatusRepository.findByContextAndExtension(context, pack);
    }

    @Transactional
    public void update(ExtensionStatus updatedStatus) {
        extensionStatusRepository.save(updatedStatus);
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(updatedStatus.getContext(),
                                                                         updatedStatus.getExtensionKey(),
                                                                         extensionConfigurationSerializer
                                                                                 .deserialize(updatedStatus
                                                                                                      .getConfiguration()),
                                                                         updatedStatus.isActive()));
    }

    @Transactional
    public void remove(ExtensionStatus updatedStatus) {
        extensionStatusRepository.deleteById(updatedStatus.getId());
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(updatedStatus.getContext(),
                                                                         updatedStatus.getExtensionKey(),
                                                                         null,
                                                                         false));
    }

    public List<ExtensionStatus> findAllByContextIn(List<String> contextNames) {
        return extensionStatusRepository.findAllByContextIn(contextNames);
    }

    @Transactional
    public void updateConfiguration(String context, String extension, String version,
                                    Map<String, Map<String, Object>> properties) {
        ExtensionPackage pack = packageRepository.findByExtensionAndVersion(extension, version)
                                                 .orElseThrow(() -> new ExtensionNotFoundException(extension, version));
        ExtensionStatus extensionStatus = extensionStatusRepository.findByContextAndExtension(context,
                                                                                              pack)
                                                                   .orElse(new ExtensionStatus(context,
                                                                                               pack));
        extensionStatus.setConfiguration(extensionConfigurationSerializer.serialize(properties));
        extensionStatusRepository.save(extensionStatus);
        applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(context,
                                                                         new ExtensionKey(extension, version),
                                                                         properties,
                                                                         extensionStatus.isActive()));
    }

    public void publishConfiguration(ExtensionPackage pack) {
        List<ExtensionStatus> contexts = extensionStatusRepository.findAllByExtension(pack);
        contexts.forEach(extensionStatus -> applicationEventPublisher
                .publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                        pack.getKey(),
                                                        extensionConfigurationSerializer
                                                                .deserialize(extensionStatus.getConfiguration()),
                                                        extensionStatus.isActive())));
    }

    @Transactional
    public void uninstall(ExtensionKey extensionKey) {
        Optional<ExtensionPackage> pack = packageRepository.findByExtensionAndVersion(extensionKey.getSymbolicName(),
                                                                                      extensionKey.getVersion());
        if (pack.isPresent()) {
            List<ExtensionStatus> contexts = extensionStatusRepository.findAllByExtension(pack.get());
            contexts.forEach(extensionStatus -> applicationEventPublisher
                    .publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                            extensionKey,
                                                            null,
                                                            false)));
            contexts.forEach(extensionStatus -> extensionStatusRepository.deleteById(extensionStatus.getId()));
        }
    }

    @Transactional
    public void removeForContext(ExtensionKey extensionKey, String context) {
        Optional<ExtensionPackage> pack = packageRepository.findByExtensionAndVersion(extensionKey.getSymbolicName(),
                                                                                      extensionKey.getVersion());
        if (!pack.isPresent()) {
            return;
        }

        Optional<ExtensionStatus> contexts =
                extensionStatusRepository.findByContextAndExtension(context, pack.get());
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
            extensionStatusRepository.deleteById(extensionStatus.getId());
            applicationEventPublisher.publishEvent(new ExtensionEnabledEvent(extensionStatus.getContext(),
                                                                             extensionStatus.getExtensionKey(),
                                                                             null,
                                                                             false));
        });
    }

    public Iterable<ExtensionInfo> listExtensions() {
        Map<ExtensionKey, ExtensionInfo> extensions = new HashMap<>();
        packageRepository.findAll()
                         .forEach(pack -> extensions.put(pack.getKey(),
                                                         new ExtensionInfo(pack.getExtension(),
                                                                           pack.getVersion(),
                                                                           pack.getFilename(),
                                                                           osgiController.getStatus(pack.getKey()))));
        extensionStatusRepository.findAll()
                                 .forEach(extension -> extensions.get(extension.getExtensionKey())
                                                                 .addContextInfo(extension.getContext(),
                                                                                 extension.isActive()));
        return extensions.values();
    }

    public List<ExtensionStatus> findAll(ExtensionKey extensionKey) {
        return packageRepository.findByExtensionAndVersion(extensionKey.getSymbolicName(), extensionKey.getVersion())
                                .map(extensionStatusRepository::findAllByExtension)
                                .orElse(Collections.emptyList());
    }
}
