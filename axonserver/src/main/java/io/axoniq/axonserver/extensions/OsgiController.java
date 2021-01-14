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
import org.osgi.framework.BundleEvent;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;

/**
 * Manages the loaded extensions and looks up services from extensions.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Controller
public class OsgiController implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(ExtensionController.class);
    private final File bundleDir;
    private final Set<Consumer<Bundle>> extensionListeners = new CopyOnWriteArraySet<>();
    private final String cacheDirectory;
    private final String cacheCleanPolicy;
    private final SystemPackagesProvider systemPackagesProvider;
    private boolean running;
    private BundleContext bundleContext;

    /**
     * Constructs an instance
     *
     * @param bundleDirectory  directory where the installed bundles are stored
     * @param cacheDirectory   OSGi cache directory
     * @param cacheCleanPolicy clean policy of the OSGi cache (none or onFirstInit)
     */
    public OsgiController(@Value("${axoniq.axonserver.extension.bundle.path:bundles}") String bundleDirectory,
                          @Value("${axoniq.axonserver.extension.cache.path:cache}") String cacheDirectory,
                          @Value("${axoniq.axonserver.extension.cache.clean:none}") String cacheCleanPolicy) {
        this.bundleDir = new File(bundleDirectory);
        this.cacheDirectory = cacheDirectory;
        this.cacheCleanPolicy = cacheCleanPolicy;
        this.systemPackagesProvider = new SystemPackagesProvider();
    }

    /**
     * Starts the {@link OsgiController}. Sets up the OSGi context and starts all bundles that are available in the
     * {@code bundleDirectory}.
     */
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
            bundleContext.addBundleListener(event -> {
                logger.debug("{}/{}: Bundle changed, type = {}, bundle state = {}",
                             event.getBundle().getSymbolicName(),
                             event.getBundle().getVersion(),
                             event.getType(),
                             event.getBundle().getState());
                if (activeStateChanged(event.getType())) {
                    extensionListeners.forEach(s -> s.accept(event.getBundle()));
                }
            });
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
                    bundle.start();
                }
            }

        } catch (Exception ex) {
            logger.error("OSGi Failed to Start", ex);
        }
    }

    private boolean activeStateChanged(int eventType) {
        return eventType == BundleEvent.STARTED || eventType == BundleEvent.STOPPED;
    }

    /**
     * Register a listener that gets invoked when extensions are activated or deactivated.
     *
     * @param listener the listener
     * @return a registration
     */
    public Registration registerExtensionListener(Consumer<Bundle> listener) {
        extensionListeners.add(listener);
        return () -> extensionListeners.remove(listener);
    }

    /**
     * Finds all services implementing a specific class. If there are multiple versions of the same extension it
     * only returns the latest version.
     *
     * @param clazz the class of the service
     * @param <T>   the type of the service
     * @return set of services implementing the service
     */
    public <T> Set<T> getServices(Class<T> clazz) {
        try {
            Collection<ServiceReference<T>> serviceReferences = bundleContext.getServiceReferences(clazz, null);
            return serviceReferences.stream()
                                    .map(s -> bundleContext.getService(s))
                                    .collect(Collectors.toSet());
        } catch (InvalidSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends Ordered> Set<ServiceWithInfo<T>> getServicesWithInfo(Class<T> clazz) {
        try {
            Collection<ServiceReference<T>> serviceReferences = bundleContext.getServiceReferences(clazz, null);
            return serviceReferences.stream()
                                    .map(s -> new ServiceWithInfo<>(bundleContext.getService(s),
                                                                    extensionKey(s.getBundle())))
                                    .collect(Collectors.toSet());
        } catch (InvalidSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<ConfigurationListener> getConfigurationListeners(ExtensionKey extensionKey) {
        try {
            Collection<ServiceReference<ConfigurationListener>> serviceReferences = bundleContext.getServiceReferences(
                    ConfigurationListener.class,
                    null);
            return serviceReferences.stream()
                                    .filter(s -> new ExtensionKey(s.getBundle().getSymbolicName(),
                                                                  s.getBundle().getVersion().toString())
                                            .equals(extensionKey))
                                    .map(s -> bundleContext.getService(s))
                                    .collect(Collectors.toSet());
        } catch (InvalidSyntaxException e) {
            throw new RuntimeException(e);
        }
    }


    private ExtensionKey extensionKey(Bundle bundle) {
        return new ExtensionKey(bundle.getSymbolicName(), bundle.getVersion().toString());
    }

    /**
     * Returns a list of all installed extensions.
     *
     * @return list of all installed extensions
     */
    public Iterable<Bundle> listExtensions() {
        return Arrays.stream(bundleContext.getBundles())
                     .filter(b -> !Objects.isNull(b.getSymbolicName()))
                     .filter(b -> !b.getSymbolicName().contains("org.apache.felix"))
                     .collect(Collectors.toList());
    }

    private String status(Bundle b) {
        switch (b.getState()) {
            case Bundle.ACTIVE:
                return "Active";
            case Bundle.INSTALLED:
                return "Installed";
            case Bundle.RESOLVED:
                return "Resolved";
            case Bundle.STARTING:
                return "Starting";
            case Bundle.STOPPING:
                return "Stopping";
            case Bundle.UNINSTALLED:
                return "Uninstalled";
        }
        return "Unknown (" + b.getState() + ")";
    }

    /**
     * Stops the controller, uninstalls all the loaded extensions.
     */
    public void stop() {
        running = false;
        for (Bundle bundle : bundleContext.getBundles()) {
            try {
                if (!bundle.getSymbolicName().contains("org.apache.felix")) {
                    logger.info("{}: stopping bundle", bundle.getSymbolicName());
                    bundle.uninstall();
                }
            } catch (Exception e) {
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

    /**
     * Adds an extension to Axon Server. If an extension with the same symbolic name and version already exists, it
     * will be replaced.
     *
     * @param fileName          the name of the file to save the extensions
     * @param bundleInputStream input stream to the extension classes
     */
    public ExtensionKey addExtension(String fileName, InputStream bundleInputStream) {
        File target = new File(bundleDir.getAbsolutePath() + File.separatorChar + fileName);
        try {
            writeToFile(bundleInputStream, target);
            ExtensionKey bundleInfo = getBundleInfo(target);

            Optional<Bundle> current = findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion());
            try (InputStream is = new FileInputStream(target)) {
                if (!current.isPresent()) {
                    Bundle bundle = bundleContext.installBundle(target.getAbsolutePath(), is);
                    bundle.start();
                    logger.info("adding bundle {}/{}", bundleInfo.getSymbolicName(), bundleInfo.getVersion());
                } else {
                    logger.info("updating bundle {}/{}", bundleInfo.getSymbolicName(), bundleInfo.getVersion());
                    current.get().update(is);
                    extensionListeners.forEach(s -> s.accept(current.get()));
                }
            }
            return bundleInfo;
        } catch (BundleException bundleException) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Could not install extension " + fileName,
                                                 bundleException);
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Could not open extension " + fileName, ioException);
        }
    }

    private ExtensionKey getBundleInfo(File target) throws IOException {
        try (JarInputStream jarInputStream = new JarInputStream(new FileInputStream(target))) {
            Attributes mainAttributes = jarInputStream.getManifest().getMainAttributes();
            String symbolicName = mainAttributes.getValue("Bundle-SymbolicName");
            String version = mainAttributes.getValue("Bundle-Version");
            if (symbolicName == null || version == null) {
                throw new MessagingPlatformException(ErrorCode.OTHER, "Missing attribute in manifest");
            }
            return new ExtensionKey(symbolicName, version);
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

    public boolean hasBundle(String symbolicName, String version, String filename) {
        return findBundle(symbolicName, version)
                .map(b -> checkFilename(b, filename))
                .isPresent();
    }

    private boolean checkFilename(Bundle bundle, String filename) {
        File file = new File(bundle.getLocation());
        return filename.equals(file.getName());
    }

    private Optional<Bundle> findBundle(String symbolicName, String version) {
        Version version1 = Version.parseVersion(version);
        Bundle[] bundles = bundleContext.getBundles();
        if (bundles != null) {
            for (Bundle bundle : bundles) {
                if (symbolicName.equals(bundle.getSymbolicName()) &&
                        version1.equals(bundle.getVersion())) {
                    return Optional.of(bundle);
                }
            }
        }
        return Optional.empty();
    }

    private void uninstallBundle(Bundle bundle) {
        if (bundle != null) {
            try {
                bundle.stop();
                bundle.uninstall();
                FileUtils.delete(new File(bundle.getLocation()));
                extensionListeners.forEach(s -> s.accept(bundle));
            } catch (BundleException bundleException) {
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     "Could not uninstall extension " + bundle.getLocation(),
                                                     bundleException);
            }
        }
    }

    /**
     * @param clazz the class of the service to find
     * @param <T>   the class of the service to find
     * @return a service
     */
    public <T> Optional<T> get(Class<T> clazz) {
        ServiceReference<T> ref = bundleContext.getServiceReference(clazz);
        return ref == null ? Optional.empty() : Optional.ofNullable(bundleContext.getService(ref));
    }

    /**
     * Stops and uninstalls a version of an extension if this exists. If an earlier version
     * of the extension exists, this will become active.
     *
     * @param bundleInfo the name and version of the extension
     */
    public void uninstallExtension(ExtensionKey bundleInfo) {
        findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion())
                .ifPresent(this::uninstallBundle);
    }

    public Bundle getBundle(ExtensionKey bundleInfo) {
        return findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion()).orElse(null);
    }

    public void updateStatus(ExtensionKey bundleInfo, boolean active) {
        findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion())
                .ifPresent(bundle -> {
                    if (active) {
                        start(bundle);
                    } else {
                        stop(bundle);
                    }
                });
    }

    private void stop(Bundle bundle) {
        if (isActive(bundle)) {
            try {
                bundle.stop();
            } catch (BundleException bundleException) {
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     String.format("Could not stop %s/%s", bundle.getSymbolicName(),
                                                                   bundle.getVersion()),
                                                     bundleException);
            }
        }
    }

    private void start(Bundle bundle) {
        if (!isActive(bundle)) {
            try {
                bundle.start();
            } catch (BundleException bundleException) {
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     String.format("Could not stop %s/%s", bundle.getSymbolicName(),
                                                                   bundle.getVersion()),
                                                     bundleException);
            }
        }
    }

    public boolean isActive(Bundle bundle) {
        if (bundle.getState() == Bundle.ACTIVE) {
            return true;
        }
        return false;
    }

    public File getLocation(ExtensionKey key) {
        return findBundle(key.getSymbolicName(), key.getVersion())
                .map(bundle -> new File(bundle.getLocation())).
                        orElseThrow(() -> new MessagingPlatformException(ErrorCode.OTHER, "Bundle not found"));
    }
}