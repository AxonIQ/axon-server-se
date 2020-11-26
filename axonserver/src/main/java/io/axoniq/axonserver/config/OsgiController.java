/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Controller
public class OsgiController implements SmartLifecycle {

    private final String[] systemPackageNames = {
            "io.axoniq.axonserver.extensions",
            "io.axoniq.axonserver.extensions.interceptor",
            "io.axoniq.axonserver.extensions.transform",
            "io.axoniq.axonserver.extensions.hook",
            "io.axoniq.axonserver.grpc",
            "io.axoniq.axonserver.grpc.command",
            "io.axoniq.axonserver.grpc.control",
            "io.axoniq.axonserver.grpc.event",
            "io.axoniq.axonserver.grpc.query"};

    private final Logger logger = LoggerFactory.getLogger(OsgiController.class);
    private boolean running;
    private BundleContext bundleContext;

    private final String bundleDirectory;
    private final String systemPackages;
    private final Set<ServiceInstalledListener> serviceInstalledListeners = new CopyOnWriteArraySet<>();
    private final Map<String, Version> latestVersions = new ConcurrentHashMap<>();

    public OsgiController(@Value("${axoniq.axonserver.bundle.path:bundles}") String bundleDirectory,
                          @Value("${axoniq.axonserver.bundle.version:4.5.0}") String version) {
        this.bundleDirectory = bundleDirectory;

        this.systemPackages = Arrays.stream(systemPackageNames)
                                    .map(s -> String.format("%s;version=\"%s\"", s, version))
                                    .collect(
                                            Collectors.joining(","));
    }

    public void start() {
        running = true;
        Map<String, String> osgiConfig = new HashMap<>();
        osgiConfig.put(Constants.FRAMEWORK_STORAGE, "cache");
        osgiConfig.put(Constants.FRAMEWORK_STORAGE_CLEAN, "onFirstInit");
        osgiConfig.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, systemPackages);
        logger.debug("System packages {}", systemPackages);
        try {
            FrameworkFactory frameworkFactory = ServiceLoader.load(FrameworkFactory.class)
                                                             .iterator().next();
            Framework framework = frameworkFactory.newFramework(osgiConfig);
            framework.start();

            bundleContext = framework.getBundleContext();
            BundleManager bundleManager = new BundleManager(bundleContext);
            bundleManager.load();
        } catch (Exception ex) {
            logger.warn("OSGi Failed to Start", ex);
        }
    }

    public Registration registerServiceListener(ServiceInstalledListener listener) {
        serviceInstalledListeners.add(listener);
        return () -> serviceInstalledListeners.remove(listener);
    }

    public <T> T getService(Class<T> clazz) {
        return bundleContext.getService(bundleContext.getServiceReference(clazz));
    }

    public <T> Iterable<T> getServices(Class<T> clazz) {
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
        logger.warn("Comparing {} version {} with {} - result {}", s.getSymbolicName(),
                    s.getVersion(),
                    latestVersions.get(s.getSymbolicName()),
                    s.getVersion().equals(latestVersions.get(s.getSymbolicName()))
        );
        return s.getVersion().equals(latestVersions.get(s.getSymbolicName()));
    }

    public Iterable<BundleInfo> listBundles() {
        return Arrays.stream(bundleContext.getBundles())
                     .filter(b -> !b.getSymbolicName().contains("org.apache.felix"))
                     .map(b -> new BundleInfo(b, latestVersion(b)))
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

    public void addBundle(String fileName, InputStream bundleInputStream) throws IOException, BundleException {
        File bundleDir = new File(bundleDirectory);
        Files.createDirectories(bundleDir.toPath());
        File target = new File(
                bundleDir.getAbsolutePath() + File.separatorChar + fileName);
        try (FileOutputStream fileOutputStream = new FileOutputStream(target)) {
            byte[] buffer = new byte[5000];
            int read = bundleInputStream.read(buffer);
            while (read > 0) {
                fileOutputStream.write(buffer, 0, read);
                read = bundleInputStream.read(buffer);
            }
        }

        Bundle current = null;
        try (JarInputStream jarInputStream = new JarInputStream(new FileInputStream(target))) {
            Attributes mainAttributes = jarInputStream.getManifest().getMainAttributes();
            String symbolicName = mainAttributes.getValue("Bundle-SymbolicName");
            String version = mainAttributes.getValue("Bundle-Version");
            Version version1 = Version.parseVersion(version);
            current = findBundle(symbolicName, version1);

            try (InputStream is = new FileInputStream(target)) {
                if (current == null) {
                    Bundle bundle = bundleContext.installBundle(target.getAbsolutePath(), is);
                    logger.info("adding bundle {}/{}", symbolicName, version1);
                    bundle.start();
                    updateLatestVersion(bundle);
                    serviceInstalledListeners.forEach(s -> s.accept(bundle));
                } else {
                    logger.info("updating bundle {}/{}", symbolicName, version1);
                    current.update(is);
                }
            }
        }
    }

    private Bundle findBundle(String symbolicName, Version version) {
        Bundle[] bundles = bundleContext.getBundles();
        if (bundles != null) {
            for (Bundle bundle : bundles) {
                if (symbolicName.equals(bundle.getSymbolicName()) &&
                        version.equals(bundle.getVersion())) {
                    return bundle;
                }
            }
        }
        return null;
    }

    public void uninstallExtension(long id) throws BundleException {
        Bundle bundle = bundleContext.getBundle(id);
        if (bundle != null) {
            bundle.stop();
            bundle.uninstall();
            FileUtils.delete(new File(bundle.getLocation()));
            latestVersions.remove(bundle.getSymbolicName());
            setLatestVersion(bundle.getSymbolicName());
            serviceInstalledListeners.forEach(s -> s.accept(bundle));
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

    private class BundleManager {

        private final BundleContext bundleContext;

        public BundleManager(BundleContext bundleContext) {

            this.bundleContext = bundleContext;
        }

        public void load() throws IOException, BundleException {
            ArrayList<Bundle> availableBundles = new ArrayList<Bundle>();
            //get and open available bundles
            for (URL url : getBundles()) {
                logger.info("Loading bundle: {}", url);
                Bundle bundle = bundleContext.installBundle(url.getFile(), url.openStream());
                availableBundles.add(bundle);
            }

            //start the bundles
            for (Bundle bundle : availableBundles) {
                try {
                    bundle.start();
                    updateLatestVersion(bundle);
                } catch (Exception ex) {
                    logger.warn("{}: Failed to start bundle", bundle.getSymbolicName(), ex);
                }
            }
        }

        private List<URL> getBundles() throws MalformedURLException {
            List<URL> bundleURLs = new ArrayList<>();
            File bundleDir = new File(bundleDirectory);
            if (bundleDir.exists() && bundleDir.isDirectory()) {
                File[] bundles = bundleDir.listFiles((dir, name) -> name.endsWith(".jar"));
                if (bundles != null) {
                    for (File bundle : bundles) {
                        bundleURLs.add(new URL("file:" + bundle.getAbsolutePath()));
                    }
                }
            }
            return bundleURLs;
        }
    }

    private void updateLatestVersion(Bundle bundle) {
        latestVersions.compute(bundle.getSymbolicName(), (name, oldversion) -> {
            if (oldversion == null || oldversion.compareTo(bundle.getVersion()) < 0) {
                return bundle.getVersion();
            }
            return oldversion;
        });
    }

    @FunctionalInterface
    public interface ServiceInstalledListener extends Consumer<Bundle> {

    }
}