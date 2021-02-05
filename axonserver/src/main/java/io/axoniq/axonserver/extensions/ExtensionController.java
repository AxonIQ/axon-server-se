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
import java.util.List;
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
     * @param fileName    the name of the extension file
     * @param inputStream input stream for the jar file for the extension
     * @return
     */
    ExtensionKey addExtension(String fileName, InputStream inputStream);

    List<ExtensionPropertyGroup> listProperties(ExtensionKey extensionKey, String context);

    void updateConfiguration(ExtensionKey extensionKey, String context, Map<String, Map<String, Object>> properties);

    void updateExtensionStatus(ExtensionKey extensionKey, String context, boolean active);

    void unregisterExtensionForContext(ExtensionKey extensionKey, String context);
}
