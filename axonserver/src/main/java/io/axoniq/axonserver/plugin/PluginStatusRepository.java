/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * Repository of plugin configuration and status per context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface PluginStatusRepository extends JpaRepository<ContextPluginStatus, Long> {

    Optional<ContextPluginStatus> findByContextAndPlugin(String context, PluginPackage pluginPackage);

    Optional<ContextPluginStatus> findByContextAndPlugin_NameAndActive(String context, String pluginName,
                                                                       boolean active);

    List<ContextPluginStatus> findAllByContextIn(List<String> context);

    List<ContextPluginStatus> findAllByPlugin(PluginPackage pluginPackage);
}