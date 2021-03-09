/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * Stores the configuration and status of a plugin for a context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Entity
@Table(name = "context_plugin_status")
public class ContextPluginStatus {

    @Id
    @GeneratedValue
    private long id;

    private String context;

    @ManyToOne
    @JoinColumn(name = "plugin_package_id")
    private PluginPackage plugin;

    @Lob
    private String configuration;

    private boolean active;

    public ContextPluginStatus() {
    }

    public ContextPluginStatus(String context, PluginPackage pluginPackage) {
        this.context = context;
        this.plugin = pluginPackage;
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

    public PluginPackage getPlugin() {
        return plugin;
    }

    public void setPlugin(PluginPackage plugin) {
        this.plugin = plugin;
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

    public PluginKey getPluginKey() {
        return plugin.getKey();
    }
}
