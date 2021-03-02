/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import java.util.Objects;

/**
 * Identification of a plugin.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class PluginKey {

    private final String symbolicName;
    private final String version;

    public PluginKey(String symbolicName, String version) {
        this.symbolicName = symbolicName;
        this.version = version;
    }

    public String getSymbolicName() {
        return symbolicName;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PluginKey that = (PluginKey) o;
        return symbolicName.equals(that.symbolicName)
                && version.equals(that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbolicName, version);
    }

    @Override
    public String toString() {
        return symbolicName + '/' + version;
    }
}
