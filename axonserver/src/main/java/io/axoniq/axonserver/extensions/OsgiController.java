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
import org.springframework.stereotype.Controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
public class OsgiController implements ExtensionServiceProvider {

    private final Logger logger = LoggerFactory.getLogger(ExtensionController.class);
    private final Set<Consumer<ExtensionKey>> extensionListeners = new CopyOnWriteArraySet<>();
    private final String cacheDirectory;
    private final String cacheCleanPolicy;
    private final SystemPackagesProvider systemPackagesProvider;
    private BundleContext bundleContext;

    /**
     * Constructs an instance
     *
     * @param cacheDirectory   OSGi cache directory
     * @param cacheCleanPolicy clean policy of the OSGi cache (none or onFirstInit)
     */
    public OsgiController(@Value("${axoniq.axonserver.extension.cache.path:cache}") String cacheDirectory,
                          @Value("${axoniq.axonserver.extension.cache.clean:none}") String cacheCleanPolicy
    ) {
        this.cacheDirectory = cacheDirectory;
        this.cacheCleanPolicy = cacheCleanPolicy;
        this.systemPackagesProvider = new SystemPackagesProvider();
    }

    /**
     * Starts the {@link OsgiController}. Sets up the OSGi context and starts all bundles that are available in the
     * {@code bundleDirectory}.
     */
    public void start() {
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
                    extensionListeners.forEach(s -> s.accept(extensionKey(event.getBundle())));
                }
            });
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
    @Override
    public Registration registerExtensionListener(Consumer<ExtensionKey> listener) {
        extensionListeners.add(listener);
        return () -> extensionListeners.remove(listener);
    }

    /**
     * Finds all services implementing a specific class. If there are multiple versions of the same extension it
     * only returns all of them.
     *
     * @param clazz the class of the service
     * @param <T>   the type of the service
     * @return set of services implementing the service
     */

    @Override
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
    public Set<ExtensionKey> listExtensions() {
        return Arrays.stream(bundleContext.getBundles())
                     .filter(b -> !Objects.isNull(b.getSymbolicName()))
                     .filter(b -> !b.getSymbolicName().contains("org.apache.felix"))
                     .map(b -> new ExtensionKey(b.getSymbolicName(), b.getVersion().toString()))
                     .collect(Collectors.toSet());
    }

    /**
     * Stops the controller, uninstalls all the loaded extensions.
     */
    public void stop() {
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

    /**
     * Adds an extension to Axon Server. If an extension with the same symbolic name and version already exists, it
     * will be replaced.
     *
     * @param extensionPackageFile the file containing the extension package
     */
    public ExtensionKey addExtension(File extensionPackageFile) {
        try {
            ExtensionKey bundleInfo = getBundleInfo(extensionPackageFile);

            Optional<Bundle> current = findBundle(bundleInfo.getSymbolicName(), bundleInfo.getVersion());

            if (!current.isPresent()) {
                try (InputStream is = new FileInputStream(extensionPackageFile)) {
                    Bundle bundle = bundleContext.installBundle(extensionPackageFile.getAbsolutePath(), is);
                    bundle.start();
                }
                logger.info("adding bundle {}/{}", bundleInfo.getSymbolicName(), bundleInfo.getVersion());
            } else {
                    logger.info("updating bundle {}/{}", bundleInfo.getSymbolicName(), bundleInfo.getVersion());
                    try (InputStream is = new FileInputStream(extensionPackageFile)) {
                        current.get().update(is);
                    }
                extensionListeners.forEach(s -> s.accept(bundleInfo));
                }

            return bundleInfo;
        } catch (BundleException bundleException) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Could not install extension " + extensionPackageFile
                                                         .getAbsolutePath(),
                                                 bundleException);
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Could not open extension " + extensionPackageFile.getAbsolutePath(),
                                                 ioException);
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

    public boolean hasBundle(String symbolicName, String version) {
        return findBundle(symbolicName, version)
                .isPresent();
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

                ExtensionKey key = extensionKey(bundle);
                extensionListeners.forEach(s -> s.accept(key));
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

    public void startExtension(String fullPath) {
        File extension = new File(fullPath);
        try (InputStream inputStream = new FileInputStream(extension)) {
            Bundle bundle = bundleContext.installBundle(extension.getAbsolutePath(), inputStream);
            logger.info("adding bundle {}/{}", bundle.getSymbolicName(), bundle.getVersion());
            bundle.start();
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, fullPath + ": Cannot read extension package", e);
        } catch (BundleException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, fullPath + ": Cannot start extension package", e);
        }
    }
}