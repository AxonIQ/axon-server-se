/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

/**
 * @author Marc Gathier
 */
@Configuration
@ConfigurationProperties(prefix = "axoniq.axonserver")
public class EmbeddedDBProperties {

    @NestedConfigurationProperty
    private StorageProperties event;

    @NestedConfigurationProperty
    private StorageProperties snapshot;

    public EmbeddedDBProperties(SystemInfoProvider systemInfoProvider) {
        event = new StorageProperties(systemInfoProvider);
        snapshot = new StorageProperties(systemInfoProvider, ".snapshots", ".sindex", ".sbloom", ".snindex", ".sxref");
    }

    public StorageProperties getEvent() {
        return event;
    }

    public void setEvent(StorageProperties event) {
        this.event = event;
    }

    public StorageProperties getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(StorageProperties snapshot) {
        this.snapshot = snapshot;
    }
}
