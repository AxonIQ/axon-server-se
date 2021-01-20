/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Lists all system bundles and the extension bundles that are available in a given directory.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ExtensionDirectoryProcessor {

    private final File bundleDir;

    public ExtensionDirectoryProcessor(File bundleDir) {
        this.bundleDir = bundleDir;
    }

    /**
     * @return a collection of files containing the user added extensions.
     */
    public List<File> getBundles() {
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
