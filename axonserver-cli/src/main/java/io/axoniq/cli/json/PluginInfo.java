/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli.json;

/**
 * Contains information about an installed plugin.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class PluginInfo {

    private String name;
    private String version;
    private PluginContextInfo[] contextInfoList;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public PluginContextInfo[] getContextInfoList() {
        return contextInfoList;
    }

    public void setContextInfoList(PluginContextInfo[] contextInfoList) {
        this.contextInfoList = contextInfoList;
    }

    public static class PluginContextInfo {

        private String context;
        private String configuration;
        private boolean active;

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        public String getConfiguration() {
            return configuration;
        }

        public void setConfiguration(String configuration) {
            this.configuration = configuration;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }
    }
}
