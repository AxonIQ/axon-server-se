/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import java.util.LinkedList;
import java.util.List;

/**
 * Describes an installed plugin.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class PluginInfo {

    private final String version;
    private final String name;
    private final String filename;
    private final String pluginStatus;
    private final List<PluginContextInfo> contextInfoList = new LinkedList<>();

    public PluginInfo(String name, String version, String filename, String pluginStatus) {
        this.version = version;
        this.name = name;
        this.filename = filename;
        this.pluginStatus = pluginStatus;
    }

    public String getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public List<PluginContextInfo> getContextInfoList() {
        return contextInfoList;
    }

    public String getFilename() {
        return filename;
    }

    public String getPluginStatus() {
        return pluginStatus;
    }

    public void addContextInfo(String context, boolean active) {
        contextInfoList.add(new PluginContextInfo(context, active));
    }
}
