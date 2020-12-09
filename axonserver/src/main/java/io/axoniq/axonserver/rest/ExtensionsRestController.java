/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.extensions.BundleInfo;
import io.axoniq.axonserver.extensions.ExtensionInfo;
import io.axoniq.axonserver.extensions.ExtensionController;
import io.axoniq.axonserver.extensions.ExtensionProperty;
import io.axoniq.axonserver.logging.AuditLog;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import springfox.documentation.annotations.ApiIgnore;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;

/**
 * REST interface to manage extensions.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@RestController
@RequestMapping("v1/extensions")
public class ExtensionsRestController {

    private static final Logger auditLog = AuditLog.getLogger();
    private final ExtensionController extensionController;

    public ExtensionsRestController(ExtensionController extensionController) {
        this.extensionController = extensionController;
    }

    @GetMapping
    public Iterable<ExtensionInfo> currentExtensions(@ApiIgnore Principal principal) {
        auditLog.info("[{}] Request to list current extensions. ",
                      AuditLog.username(principal));
        return extensionController.listExtensions();
    }

    @DeleteMapping
    public void uninstallExtension(@RequestParam String extension, String version, @ApiIgnore Principal principal) {
        auditLog.info("[{}] Request to uninstall extension {}/{}. ", AuditLog.username(principal), extension, version);
        extensionController.uninstallExtension(new BundleInfo(extension, version));
    }

    @GetMapping("configuration")
    public Iterable<ExtensionProperty> configuration(@RequestParam String extension, String version,
                                                     @ApiIgnore Principal principal) {
        auditLog.info("[{}] Request for configuration of {}/{}. ", AuditLog.username(principal), extension, version);
        return extensionController.listProperties(new BundleInfo(extension, version));
    }

    @PostMapping
    public void installExtension(@RequestParam("bundle") MultipartFile extensionBundle, @ApiIgnore Principal principal)
            throws IOException, BundleException {
        auditLog.info("[{}] Request to install extension {}. ",
                      AuditLog.username(principal),
                      extensionBundle.getOriginalFilename());
        try (InputStream inputStream = extensionBundle.getInputStream()) {
            extensionController.addExtension(extensionBundle.getOriginalFilename(), inputStream);
        }
    }

    @PostMapping("configuration")
    public void updateConfiguration(@RequestBody ExtensionConfigurationJSON configurationJSON,
                                    @ApiIgnore Principal principal) {
        auditLog.info("[{}] Request to update configuration of {}/{}. ", AuditLog.username(principal),
                      configurationJSON.getExtension(),
                      configurationJSON.getVersion());
        extensionController.updateConfiguration(new BundleInfo(configurationJSON.getExtension(),
                                                               configurationJSON.getVersion()),
                                                configurationJSON.getProperties());
    }
}
