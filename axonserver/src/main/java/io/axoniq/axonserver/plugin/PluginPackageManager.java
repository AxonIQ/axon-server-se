/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import javax.transaction.Transactional;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Optional;

/**
 * Manages the installed plugins.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class PluginPackageManager implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(PluginPackageManager.class);
    private final ApplicationEventPublisher eventPublisher;
    private final String bundleDirectory;
    private final PluginPackageRepository pluginPackageRepository;
    private final OsgiController osgiController;
    private final PluginContextManager pluginContextManager;
    private boolean running;

    public PluginPackageManager(
            PluginPackageRepository pluginPackageRepository,
            OsgiController osgiController,
            PluginContextManager pluginContextManager,
            ApplicationEventPublisher eventPublisher,
            MessagingPlatformConfiguration messagingPlatformConfiguration
    ) {
        this.pluginPackageRepository = pluginPackageRepository;
        this.osgiController = osgiController;
        this.pluginContextManager = pluginContextManager;
        this.eventPublisher = eventPublisher;
        this.bundleDirectory = messagingPlatformConfiguration.getPluginPackageDirectory();
        this.osgiController.registerPluginListener(this::pluginStatusChanged);
    }

    private void pluginStatusChanged(PluginKey pluginKey, String status) {
        eventPublisher.publishEvent(new PluginEvent(pluginKey, status));
    }

    @Override
    public void start() {
        osgiController.start();
        try {
            Files.createDirectories(new File(bundleDirectory).toPath());
            pluginPackageRepository.findAll()
                                   .forEach(this::startPlugin);
            pluginContextManager.start();
        } catch (Exception exception) {
            logger.error("Failed to start plugin", exception);
        }
        running = true;
    }

    private void startPlugin(PluginPackage pluginPackage) {
        try {
            if (pluginPackage.isDeleted()) {
                removePlugin(pluginPackage);
                return;
            }
            osgiController.startPlugin(
                    bundleDirectory + File.separatorChar + pluginPackage
                            .getFilename());
            pluginStatusChanged(pluginPackage.getKey(), "Started");
        } catch (Exception ex) {
            logger.error("Failed to start plugin {}", pluginPackage.getKey(), ex);
        }
    }

    @Override
    public void stop() {
        osgiController.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Transactional
    public void uninstallPlugin(PluginKey pluginKey) {
        pluginContextManager.uninstall(pluginKey);
        osgiController.uninstallPlugin(pluginKey);
        synchronized (pluginPackageRepository) {
            pluginPackageRepository.findByNameAndVersion(
                    pluginKey.getSymbolicName(),
                    pluginKey.getVersion()).ifPresent(this::removePlugin);
        }
    }

    private void removePlugin(PluginPackage p) {
        pluginPackageRepository.deleteById(p.getId());
        FileUtils.delete(new File(bundleDirectory + File.separatorChar + p.getFilename()));
    }

    @Transactional
    public PluginPackage addPlugin(String fileName, InputStream inputStream) {
        File target = new File(bundleDirectory + File.separatorChar + fileName);
        writeToFile(inputStream, target);
        try {
            PluginKey pluginKey = osgiController.addPlugin(target);
            Optional<PluginPackage> pluginPackage = pluginPackageRepository.findByNameAndVersion(
                    pluginKey.getSymbolicName(),
                    pluginKey.getVersion());
            PluginPackage pack;
            if (pluginPackage.isPresent()) {
                pack = pluginPackage.get();
                if (!fileName.equals(pack.getFilename())) {
                    FileUtils.delete(new File(bundleDirectory + File.separatorChar + pack.getFilename()));
                    pack.setFilename(fileName);
                    pluginPackageRepository.save(pack);
                }
            } else {
                pack = new PluginPackage();
                pack.setName(pluginKey.getSymbolicName());
                pack.setVersion(pluginKey.getVersion());
                pack.setFilename(fileName);
                pack = pluginPackageRepository.save(pack);
            }
            pluginContextManager.publishConfiguration(pack);
            return pack;
        } catch (RuntimeException runtimeException) {
            FileUtils.delete(target);
            throw runtimeException;
        }

    }

    private void writeToFile(InputStream bundleInputStream, File target) {
        try (FileOutputStream fileOutputStream = new FileOutputStream(target)) {
            byte[] buffer = new byte[5000];
            int read = bundleInputStream.read(buffer);
            while (read > 0) {
                fileOutputStream.write(buffer, 0, read);
                read = bundleInputStream.read(buffer);
            }
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 target + ": Writing plugin stream to file failed",
                                                 ioException);
        }
    }

    public Iterable<PluginInfo> listPlugins() {
        return pluginContextManager.listPlugins();
    }

    public File getLocation(PluginKey key) {
        return pluginPackageRepository.findByNameAndVersion(key.getSymbolicName(), key.getVersion())
                                      .map(pack -> new File(pack.getFilename()))
                                      .orElse(null);
    }

    public Optional<PluginPackage> getPluginPackage(String name, String version, String filename) {
        return pluginPackageRepository.findByNameAndVersion(name, version)
                                      .map(pack -> pack.getFilename().equals(filename) ? pack : null);
    }

    public File getFullPath(String name, String version) {
        return pluginPackageRepository.findByNameAndVersion(name, version)
                                      .map(pack -> new File(
                                              bundleDirectory + File.separatorChar + pack.getFilename()))
                                      .orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER,
                                                                                        "Plugin not found"));
    }
}
