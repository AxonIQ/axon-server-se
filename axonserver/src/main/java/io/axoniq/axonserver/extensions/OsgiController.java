/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.Registration;
import io.axoniq.axonserver.localstorage.file.FileUtils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.Version;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Controller
public class OsgiController implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(OsgiController.class);
    private final File bundleDir;
    private final Set<Consumer<Bundle>> serviceInstalledListeners = new CopyOnWriteArraySet<>();
    private final Map<String, Version> latestVersions = new ConcurrentHashMap<>();
    private final String cacheDirectory;
    private final String cacheCleanPolicy;
    private final SystemPackagesProvider systemPackagesProvider;
    private final Set<BiConsumer<Bundle, String>> bundleInstalledListeners = new CopyOnWriteArraySet<>();
    private boolean running;
    private BundleContext bundleContext;

    public OsgiController(@Value("${axoniq.axonserver.extension.bundle.path:bundles}") String bundleDirectory,
                          @Value("${axoniq.axonserver.extension.cache.path:cache}") String cacheDirectory,
                          @Value("${axoniq.axonserver.extension.cache.clean:none}") String cacheCleanPolicy,
                          @Value("${axoniq.axonserver.extension.bundle.version:4.5.0}") String version) {
        this.bundleDir = new File(bundleDirectory);
        this.cacheDirectory = cacheDirectory;
        this.cacheCleanPolicy = cacheCleanPolicy;
        this.systemPackagesProvider = new SystemPackagesProvider(version);
    }

    public void start() {
        running = true;
        Map<String, String> osgiConfig = new HashMap<>();
        osgiConfig.put(Constants.FRAMEWORK_STORAGE, cacheDirectory);
        osgiConfig.put(Constants.FRAMEWORK_STORAGE_CLEAN, cacheCleanPolicy);
        osgiConfig.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, systemPackagesProvider.getSystemPackages());
        logger.debug("System packages {}", systemPackagesProvider.getSystemPackages());
        try {
            FrameworkFactory frameworkFactory = ServiceLoader.load(FrameworkFactory.class)
                                                             .iterator().next();
            Framework framework = frameworkFactory.newFramework(osgiConfig);
            framework.start();

            bundleContext = framework.getBundleContext();
            Files.createDirectories(bundleDir.toPath());
            ExtensionDirectoryProcessor bundleManager = new ExtensionDirectoryProcessor(bundleDir);
            for (URL url : bundleManager.getSystemBundles()) {
                try (InputStream inputStream = url.openStream()) {
                    Bundle bundle = bundleContext.installBundle(url.toString(), inputStream);
                    logger.info("adding bundle {}/{}", bundle.getSymbolicName(), bundle.getVersion());
                    bundle.start();
                }
            }

            for (File extension : bundleManager.getBundles()) {
                try (InputStream inputStream = new FileInputStream(extension)) {
                    Bundle bundle = bundleContext.installBundle(extension.getAbsolutePath(), inputStream);
                    logger.info("adding bundle {}/{}", bundle.getSymbolicName(), bundle.getVersion());
                    updateConfiguration(bundle, null);
                    bundle.start();
                }
            }

            setLatestVersions();
        } catch (Exception ex) {
            logger.warn("OSGi Failed to Start", ex);
        }
    }

    public Registration registerServiceListener(Consumer<Bundle> listener) {
        serviceInstalledListeners.add(listener);
        return () -> serviceInstalledListeners.remove(listener);
    }

    public <T> Set<T> getServices(Class<T> clazz) {
        try {
            Collection<ServiceReference<T>> serviceReferences = bundleContext.getServiceReferences(clazz, null);
            return serviceReferences.stream()
                                    .filter(s -> latestVersion(s.getBundle()))
                                    .map(s -> bundleContext.getService(s))
                                    .collect(Collectors.toSet());
        } catch (InvalidSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean latestVersion(Bundle s) {
        logger.trace("Comparing {} version {} with {} - result {}", s.getSymbolicName(),
                     s.getVersion(),
                     latestVersions.get(s.getSymbolicName()),
                     s.getVersion().equals(latestVersions.get(s.getSymbolicName()))
        );
        return s.getVersion().equals(latestVersions.get(s.getSymbolicName()));
    }

    public Iterable<ExtensionInfo> listBundles() {
        return Arrays.stream(bundleContext.getBundles())
                     .filter(b -> !Objects.isNull(b.getSymbolicName()))
                     .filter(b -> !b.getSymbolicName().contains("org.apache.felix"))
                     .map(b -> new ExtensionInfo(b, latestVersion(b)))
                     .collect(Collectors.toList());
    }

    public void stop() {
        running = false;
        for (Bundle bundle : bundleContext.getBundles()) {
            try {
                if (!bundle.getSymbolicName().contains("org.apache.felix")) {
                    logger.info("{}: stopping bundle", bundle.getSymbolicName());
                    bundle.uninstall();
                }
            } catch (BundleException e) {
                logger.warn("{}: uninstall failed", bundle.getSymbolicName(), e);
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    public void addExtension(String fileName, String configuration, InputStream bundleInputStream) {
        File target = new File(bundleDir.getAbsolutePath() + File.separatorChar + fileName);
        try {
            writeToFile(bundleInputStream, target);
            BundleInfo bundleInfo = getBundleInfo(target);

            Bundle current = findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion());
            try (InputStream is = new FileInputStream(target)) {
                if (current == null) {
                    Bundle bundle = bundleContext.installBundle(target.getAbsolutePath(), is);
                    logger.info("adding bundle {}/{}", bundleInfo.getSymbolicName(), bundleInfo.getVersion());
                    updateConfiguration(bundle, configuration);
                    bundle.start();
                    updateLatestVersion(bundle);
                    serviceInstalledListeners.forEach(s -> s.accept(bundle));
                } else {
                    logger.info("updating bundle {}/{}", bundleInfo.getSymbolicName(), bundleInfo.getVersion());
                    current.update(is);
                    updateConfiguration(current, configuration);
                    serviceInstalledListeners.forEach(s -> s.accept(current));
                }
            }
        } catch (BundleException bundleException) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Could not install extension " + fileName,
                                                 bundleException);
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Could not open extension " + fileName, ioException);
        }
    }

    private void updateConfiguration(Bundle bundle, String configuration) {
        bundleInstalledListeners.forEach(listener -> listener.accept(bundle, configuration));
    }

    private BundleInfo getBundleInfo(File target) throws IOException {
        try (JarInputStream jarInputStream = new JarInputStream(new FileInputStream(target))) {
            Attributes mainAttributes = jarInputStream.getManifest().getMainAttributes();
            String symbolicName = mainAttributes.getValue("Bundle-SymbolicName");
            String version = mainAttributes.getValue("Bundle-Version");
            return new BundleInfo(symbolicName, version);
        }
    }

    private void writeToFile(InputStream bundleInputStream, File target) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(target)) {
            byte[] buffer = new byte[5000];
            int read = bundleInputStream.read(buffer);
            while (read > 0) {
                fileOutputStream.write(buffer, 0, read);
                read = bundleInputStream.read(buffer);
            }
        }
    }

    private Bundle findBundle(String symbolicName, String version) {
        Version version1 = Version.parseVersion(version);
        Bundle[] bundles = bundleContext.getBundles();
        if (bundles != null) {
            for (Bundle bundle : bundles) {
                if (symbolicName.equals(bundle.getSymbolicName()) &&
                        version1.equals(bundle.getVersion())) {
                    return bundle;
                }
            }
        }
        return null;
    }

    private void uninstallBundle(Bundle bundle) {
        if (bundle != null) {
            try {
                bundle.stop();
                bundle.uninstall();
                FileUtils.delete(new File(bundle.getLocation()));
                latestVersions.remove(bundle.getSymbolicName());
                setLatestVersion(bundle.getSymbolicName());
                serviceInstalledListeners.forEach(s -> s.accept(bundle));
            } catch (BundleException bundleException) {
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     "Could not uninstall extension " + bundle.getLocation(),
                                                     bundleException);
            }
        }
    }

    private void setLatestVersion(String symbolicName) {
        Bundle[] bundles = bundleContext.getBundles();
        if (bundles != null) {
            Version latest = null;
            for (Bundle bundle : bundles) {
                if (symbolicName.equals(bundle.getSymbolicName()) &&
                        (latest == null || bundle.getVersion().compareTo(latest) > 0)) {
                    latest = bundle.getVersion();
                }
            }
            if (latest != null) {
                latestVersions.put(symbolicName, latest);
            }
        }
    }

    private void setLatestVersions() {
        Bundle[] bundles = bundleContext.getBundles();
        if (bundles != null) {
            for (Bundle bundle : bundles) {
                updateLatestVersion(bundle);
            }
        }
    }

    private void updateLatestVersion(Bundle bundle) {
        if (bundle.getSymbolicName() != null) {
            latestVersions.compute(bundle.getSymbolicName(), (name, oldversion) -> {
                if (oldversion == null || oldversion.compareTo(bundle.getVersion()) < 0) {
                    return bundle.getVersion();
                }
                return oldversion;
            });
        }
    }

    public <T> T getService(Class<T> clazz) {
        Iterator<T> candidates = getServices(clazz).iterator();
        return candidates.hasNext() ? candidates.next() : null;
    }

    public <T> T get(Class<T> clazz) {
        ServiceReference<T> ref = bundleContext.getServiceReference(clazz);
        return ref == null ? null : bundleContext.getService(ref);
    }

    /**
     * Stops and uninstalls a version of an extension if this exists. If an earlier version
     * of the extension exists, this will become active.
     *
     * @param bundleInfo the name and version of the extension
     */
    public void uninstallExtension(BundleInfo bundleInfo) {
        Optional.ofNullable(findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion()))
                .ifPresent(this::uninstallBundle);
    }

    public void registerBundleListener(BiConsumer<Bundle, String> listener) {
        bundleInstalledListeners.add(listener);
    }

    public Bundle getBundle(BundleInfo bundleInfo) {
        return findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion());
    }
}