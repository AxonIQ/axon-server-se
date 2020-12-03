/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import java.io.InputStream;

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
     * @param id the identifier of the extension
     */
    void uninstallExtension(long id);

    /**
     * Adds or updates an extension. If there is already an extension with the same name and the same version it is
     * replaced.
     * If this extension has the same name and a higher version, it will install the extension and start using this
     * version
     * from now on.
     *
     * @param fileName    the name of the extension file
     * @param inputStream input stream for the jar file for the extension
     */
    void addExtension(String fileName, InputStream inputStream);
}
