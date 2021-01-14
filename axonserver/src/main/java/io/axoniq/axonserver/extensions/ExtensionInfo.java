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
    private final List<ExtensionContextInfo> contextInfoList = new LinkedList<>();

    public ExtensionInfo(String name, String version) {
        this.version = version;
        this.name = name;
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

    public void addContextInfo(String context, String configuration, boolean active) {
        contextInfoList.add(new ExtensionContextInfo(context, configuration, active));
    }
}
