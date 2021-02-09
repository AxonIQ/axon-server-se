/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import java.util.LinkedList;
import java.util.List;

/**
 * Describes an installed extension.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ExtensionInfo {

    private final String version;
    private final String name;
    private final String filename;
    private final String extensionStatus;
    private final List<ExtensionContextInfo> contextInfoList = new LinkedList<>();

    public ExtensionInfo(String name, String version, String filename, String extensionStatus) {
        this.version = version;
        this.name = name;
        this.filename = filename;
        this.extensionStatus = extensionStatus;
    }

    public String getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public List<ExtensionContextInfo> getContextInfoList() {
        return contextInfoList;
    }

    public String getFilename() {
        return filename;
    }

    public String getExtensionStatus() {
        return extensionStatus;
    }

    public void addContextInfo(String context, boolean active) {
        contextInfoList.add(new ExtensionContextInfo(context, active));
    }
}
