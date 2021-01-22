/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * @author Marc Gathier
 */
@Entity
@Table(name = "extension")
public class ExtensionStatus {

    @Id
    @GeneratedValue
    private long id;

    private String context;

    @ManyToOne
    @JoinColumn(name = "extension_package_id")
    private ExtensionPackage extension;

    @Lob
    private String configuration;

    private boolean active;

    public ExtensionStatus() {
    }

    public ExtensionStatus(String context, ExtensionPackage extension) {
        this.context = context;
        this.extension = extension;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public ExtensionPackage getExtension() {
        return extension;
    }

    public void setExtension(ExtensionPackage extension) {
        this.extension = extension;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(String configuration) {
        this.configuration = configuration;
    }

    public ExtensionKey getExtensionKey() {
        return extension.getKey();
    }
}
