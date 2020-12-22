/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.rest.ExtensionPropertyGroup;

import java.io.InputStream;
import java.util.Map;

/**
 * Manages the extensions.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface ExtensionController {

    /**
     * @return iterator of the currently installed extensions
     */
    Iterable<ExtensionInfo> listExtensions();

    /**
     * Uninstalls an extension from Axon Server.
     *
     * @param extensionKey the name and version of the extension
     */
    void uninstallExtension(ExtensionKey extensionKey);

    /**
     * Adds or updates an extension. If there is already an extension with the same name and the same version it is
     * replaced.
     * If this extension has the same name and a higher version, it will install the extension and start using this
     * version
     * from now on.
     *
     * @param fileName      the name of the extension file
     * @param configuration JSON string containing configuration properties to set before starting the extension
     * @param start         start the extension on install
     * @param inputStream   input stream for the jar file for the extension
     */
    void addExtension(String fileName, String configuration, boolean start, InputStream inputStream);

    Iterable<ExtensionPropertyGroup> listProperties(ExtensionKey extensionKey);

    void updateConfiguration(ExtensionKey extensionKey, Map<String, Map<String, Object>> properties);

    void updateExtensionState(ExtensionKey extensionKey, boolean active);
}
