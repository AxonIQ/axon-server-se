/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.rest;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.refactoring.messaging.MessagingPlatformException;
import io.axoniq.axonserver.refactoring.plugin.PluginController;
import io.axoniq.axonserver.refactoring.plugin.PluginInfo;
import io.axoniq.axonserver.refactoring.plugin.PluginKey;
import io.axoniq.axonserver.refactoring.security.AuditLog;
import io.axoniq.axonserver.refactoring.transport.rest.dto.PluginConfigurationJSON;
import io.axoniq.axonserver.refactoring.transport.rest.dto.PluginPropertyGroup;
import org.slf4j.Logger;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import springfox.documentation.annotations.ApiIgnore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;

import static io.axoniq.axonserver.refactoring.util.StringUtils.sanitize;

/**
 * REST interface to manage plugins.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@RestController
@RequestMapping("v1/plugins")
public class PluginsRestController {

    private static final Logger auditLog = AuditLog.getLogger();
    public static final String PLUGINS_DISABLED = "Plugins disabled";
    private final PluginController pluginController;
    private final boolean pluginsEnabled;

    public PluginsRestController(PluginController pluginController,
                                 MessagingPlatformConfiguration configuration) {
        this.pluginsEnabled = configuration.isPluginsEnabled();
        this.pluginController = pluginController;
    }

    @GetMapping
    public Iterable<PluginInfo> currentPlugins(@ApiIgnore Principal principal) {
        if (!pluginsEnabled) {
            throw new MessagingPlatformException(ErrorCode.PLUGINS_DISABLED, PLUGINS_DISABLED);
        }
        auditLog.info("[{}] Request to list current plugins. ",
                      AuditLog.username(principal));
        return pluginController.listPlugins();
    }

    @DeleteMapping
    public void uninstallPlugin(@RequestParam String name, @RequestParam String version,
                                @ApiIgnore Principal principal) {
        if (!pluginsEnabled) {
            throw new MessagingPlatformException(ErrorCode.PLUGINS_DISABLED, PLUGINS_DISABLED);
        }
        auditLog.info("[{}] Request to uninstall plugin {}/{}. ", AuditLog.username(principal),
                      sanitize(name),
                      sanitize(version));
        pluginController.uninstallPlugin(new PluginKey(name, version));
    }

    @PostMapping("status")
    public void updateStatus(@RequestParam String name,
                             @RequestParam String version,
                             @RequestParam(required = false, name = "targetContext") String context,
                             @RequestParam boolean active,
                             @ApiIgnore Principal principal) {
        if (!pluginsEnabled) {
            throw new MessagingPlatformException(ErrorCode.PLUGINS_DISABLED, PLUGINS_DISABLED);
        }
        auditLog.info("[{}] Request to {} plugin {}/{}. ",
                      AuditLog.username(principal),
                      active ? "start" : "stop",
                      sanitize(name),
                      sanitize(version));
        pluginController.updatePluginStatus(new PluginKey(name, version), context, active);
    }

    @DeleteMapping("context")
    public void unregisterPluginForContext(@RequestParam String name,
                                           @RequestParam String version,
                                           @RequestParam(required = false, name = "targetContext") String context,
                                           @ApiIgnore Principal principal) {
        if (!pluginsEnabled) {
            throw new MessagingPlatformException(ErrorCode.PLUGINS_DISABLED, PLUGINS_DISABLED);
        }
        auditLog.info("[{}] Request to unregister plugin {}/{} for context {}.",
                      AuditLog.username(principal),
                      sanitize(name),
                      sanitize(version),
                      sanitize(context));
        pluginController.unregisterPluginForContext(new PluginKey(name, version), context);
    }


    @GetMapping("configuration")
    public Iterable<PluginPropertyGroup> configuration(@RequestParam String name,
                                                       @RequestParam String version,
                                                       @RequestParam(required = false, name = "targetContext") String context,
                                                       @ApiIgnore Principal principal) {
        if (!pluginsEnabled) {
            throw new MessagingPlatformException(ErrorCode.PLUGINS_DISABLED, PLUGINS_DISABLED);
        }
        auditLog.info("[{}] Request for configuration of {}/{}. ", AuditLog.username(principal),
                      sanitize(name),
                      sanitize(version));
        return pluginController.listProperties(new PluginKey(name, version), context);
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public PluginKey installPlugin(@RequestPart("bundle") MultipartFile pluginBundle,
                                   @ApiIgnore Principal principal)
            throws IOException {
        if (!pluginsEnabled) {
            throw new MessagingPlatformException(ErrorCode.PLUGINS_DISABLED, PLUGINS_DISABLED);
        }
        auditLog.info("[{}] Request to install plugin {}. ",
                      AuditLog.username(principal),
                      sanitize(pluginBundle.getOriginalFilename()));

        try (InputStream inputStream = pluginBundle.getInputStream()) {
            String effectiveFilename = uniqueName(pluginBundle.getOriginalFilename());
            return pluginController.addPlugin(effectiveFilename, inputStream);
        }
    }

    private String uniqueName(String originalFilename) {
        if (originalFilename == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "No plugin package provided");
        }
        if (originalFilename.contains(File.separator)) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "Filename should not contain directory separator");
        }
        int lastDot = originalFilename.lastIndexOf('.');
        return lastDot > 0 ? originalFilename.substring(0, lastDot) + "-" + System.currentTimeMillis()
                + originalFilename.substring(lastDot)
                : originalFilename + "-" + System.currentTimeMillis();
    }

    @PostMapping("configuration")
    public void updateConfiguration(@RequestBody PluginConfigurationJSON configurationJSON,
                                    @ApiIgnore Principal principal) {
        if (!pluginsEnabled) {
            throw new MessagingPlatformException(ErrorCode.PLUGINS_DISABLED, PLUGINS_DISABLED);
        }
        auditLog.info("[{}] Request to update configuration of {}/{}. ", AuditLog.username(principal),
                      sanitize(configurationJSON.getName()),
                      sanitize(configurationJSON.getVersion()));
        pluginController.updateConfiguration(new PluginKey(configurationJSON.getName(),
                                                           configurationJSON.getVersion()),
                                             configurationJSON.getContext(),
                                             configurationJSON.getProperties());
    }
}
