/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Loads all extension bundles that are available in a given directory.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ExtensionDirectoryProcessor {

    private final Logger logger = LoggerFactory.getLogger(ExtensionDirectoryProcessor.class);
    private final BundleContext bundleContext;
    private final File bundleDir;

    public ExtensionDirectoryProcessor(BundleContext bundleContext, File bundleDir) {
        this.bundleContext = bundleContext;
        this.bundleDir = bundleDir;
    }

    /**
     * Attempts to load and start all bundles (jar files) in the given directory. If loading or starting a bundle fails,
     * it logs a warning and continues with the next one.
     */
    public void initBundles() {
        ArrayList<Bundle> availableBundles = new ArrayList<>();
        //get and open available bundles
        for (File file : getBundles()) {
            logger.info("Loading bundle: {}", file);
            try (InputStream inputStream = new FileInputStream(file)) {
                Bundle bundle = bundleContext.installBundle(file.getAbsolutePath(), inputStream);
                availableBundles.add(bundle);
            } catch (Exception ex) {
                logger.warn("{}: Failed to install bundle", file.getAbsolutePath(), ex);
            }
        }

        //start the bundles
        for (Bundle bundle : availableBundles) {
            try {
                bundle.start();
            } catch (Exception ex) {
                logger.warn("{}: Failed to start bundle", bundle.getSymbolicName(), ex);
            }
        }
    }

    private List<File> getBundles() {
        List<File> bundleURLs = new ArrayList<>();
        if (bundleDir.exists() && bundleDir.isDirectory()) {
            File[] bundles = bundleDir.listFiles((dir, name) -> name.endsWith(".jar"));
            if (bundles != null) {
                bundleURLs.addAll(Arrays.asList(bundles));
            }
        }
        return bundleURLs;
    }
}
