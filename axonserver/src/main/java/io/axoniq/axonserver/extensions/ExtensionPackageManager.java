/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.file.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Optional;
import javax.transaction.Transactional;

/**
 * @author Marc Gathier
 */
@Component
public class ExtensionPackageManager implements SmartLifecycle {

    private final ApplicationEventPublisher eventPublisher;
    private final String bundleDirectory;
    private final ExtensionPackageRepository extensionPackageRepository;
    private final OsgiController osgiController;
    private final ExtensionContextManager extensionContextManager;
    private boolean running;

    public ExtensionPackageManager(
            ExtensionPackageRepository extensionPackageRepository,
            OsgiController osgiController,
            ExtensionContextManager extensionContextManager,
            ApplicationEventPublisher eventPublisher,
            @Value("${axoniq.axonserver.extension.bundle.path:bundles}") String bundleDirectory
    ) {
        this.extensionPackageRepository = extensionPackageRepository;
        this.osgiController = osgiController;
        this.extensionContextManager = extensionContextManager;
        this.eventPublisher = eventPublisher;
        this.bundleDirectory = bundleDirectory;
    }

    @Override
    public void start() {
        osgiController.start();
        try {
            Files.createDirectories(new File(bundleDirectory).toPath());
            extensionPackageRepository.findAll()
                                      .forEach(extensionPackage ->
                                                       osgiController.startExtension(
                                                               bundleDirectory + File.separatorChar + extensionPackage
                                                                       .getFilename()));
            extensionContextManager.start();
        } catch (Exception ioException) {
            ioException.printStackTrace();
        }
        running = true;
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
    public void uninstallExtension(ExtensionKey extensionKey) {
        extensionContextManager.uninstall(extensionKey);
        osgiController.uninstallExtension(extensionKey);
        extensionPackageRepository.findByExtensionAndVersion(
                extensionKey.getSymbolicName(),
                extensionKey.getVersion()).ifPresent(p -> {
            extensionPackageRepository.deleteById(p.getId());
            FileUtils.delete(new File(bundleDirectory + File.separatorChar + p.getFilename()));
            eventPublisher.publishEvent(new ExtensionEvent(p.getKey()));
        });
    }

    @Transactional
    public ExtensionPackage addExtension(String fileName, InputStream inputStream) {
        File target = new File(bundleDirectory + File.separatorChar + fileName);
        writeToFile(inputStream, target);
        ExtensionKey extensionKey = osgiController.addExtension(target);
        Optional<ExtensionPackage> extensionPackage = extensionPackageRepository.findByExtensionAndVersion(
                extensionKey.getSymbolicName(),
                extensionKey.getVersion());
        ExtensionPackage pack;
        if (extensionPackage.isPresent()) {
            pack = extensionPackage.get();
            if (!fileName.equals(pack.getFilename())) {
                FileUtils.delete(new File(bundleDirectory + File.separatorChar + pack.getFilename()));
                pack.setFilename(fileName);
                extensionPackageRepository.save(pack);
            }
        } else {
            pack = new ExtensionPackage();
            pack.setExtension(extensionKey.getSymbolicName());
            pack.setVersion(extensionKey.getVersion());
            pack.setFilename(fileName);
            pack = extensionPackageRepository.save(pack);
        }
        extensionContextManager.publishConfiguration(pack);
        eventPublisher.publishEvent(new ExtensionEvent(extensionKey));
        return pack;
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
                                                 target + ": Writing extension stream to file failed",
                                                 ioException);
        }
    }

    public Iterable<ExtensionInfo> listExtensions() {
        return extensionContextManager.listExtensions();
    }

    public File getLocation(ExtensionKey key) {
        return extensionPackageRepository.findByExtensionAndVersion(key.getSymbolicName(), key.getVersion())
                                         .map(pack -> new File(pack.getFilename()))
                                         .orElse(null);
    }

    public Optional<ExtensionPackage> getExtension(String name, String version, String filename) {
        return extensionPackageRepository.findByExtensionAndVersion(name, version)
                                         .map(pack -> pack.getFilename().equals(filename) ? pack : null);
    }

    public File getFile(String name, String version) {
        return extensionPackageRepository.findByExtensionAndVersion(name, version)
                                         .map(pack -> new File(
                                                 bundleDirectory + File.separatorChar + pack.getFilename()))
                                         .orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER,
                                                                                           "Extension not found"));
    }
}
