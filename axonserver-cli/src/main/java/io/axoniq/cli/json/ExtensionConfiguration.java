/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli.json;

import java.util.HashMap;
import java.util.Map;

/**
 * Object to pass configuration for an extension in a context to Axon Server.
 * The configuration is a map of maps per configuration listener.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class ExtensionConfiguration {

    private String context;
    private String extension;
    private String version;
    private Map<String, Map<String, Object>> properties = new HashMap<>();

    public ExtensionConfiguration(String extension, String version, String context) {
        this.extension = extension;
        this.version = version;
        this.context = context;
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, Map<String, Object>> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Map<String, Object>> properties) {
        this.properties = properties;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public void addProperty(String group, String name, String value) {
        properties.computeIfAbsent(group, g -> new HashMap<>())
                  .put(name, value);
    }
}
