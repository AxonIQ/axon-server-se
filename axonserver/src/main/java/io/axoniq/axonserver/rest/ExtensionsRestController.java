/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.ExtensionController;
import io.axoniq.axonserver.extensions.ExtensionInfo;
import io.axoniq.axonserver.extensions.ExtensionKey;
import io.axoniq.axonserver.logging.AuditLog;
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

import static io.axoniq.axonserver.util.StringUtils.sanitize;

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
    public static final String EXTENSIONS_DISABLED = "Extensions disabled";
    private final ExtensionController extensionController;
    private final boolean extensionsEnabled;

    public ExtensionsRestController(ExtensionController extensionController,
                                    MessagingPlatformConfiguration configuration) {
        this.extensionsEnabled = configuration.isExtensionsEnabled();
        this.extensionController = extensionController;
    }

    @GetMapping
    public Iterable<ExtensionInfo> currentExtensions(@ApiIgnore Principal principal) {
        if (!extensionsEnabled) {
            throw new MessagingPlatformException(ErrorCode.EXTENSIONS_DISABLED, EXTENSIONS_DISABLED);
        }
        auditLog.info("[{}] Request to list current extensions. ",
                      AuditLog.username(principal));
        return extensionController.listExtensions();
    }

    @DeleteMapping
    public void uninstallExtension(@RequestParam String extension, @RequestParam String version,
                                   @ApiIgnore Principal principal) {
        if (!extensionsEnabled) {
            throw new MessagingPlatformException(ErrorCode.EXTENSIONS_DISABLED, EXTENSIONS_DISABLED);
        }
        auditLog.info("[{}] Request to uninstall extension {}/{}. ", AuditLog.username(principal),
                      sanitize(extension),
                      sanitize(version));
        extensionController.uninstallExtension(new ExtensionKey(extension, version));
    }

    @PostMapping("status")
    public void updateStatus(@RequestParam String extension,
                             @RequestParam String version,
                             @RequestParam(required = false, name = "targetContext") String context,
                             @RequestParam boolean active,
                             @ApiIgnore Principal principal) {
        if (!extensionsEnabled) {
            throw new MessagingPlatformException(ErrorCode.EXTENSIONS_DISABLED, EXTENSIONS_DISABLED);
        }
        auditLog.info("[{}] Request to {} extension {}/{}. ",
                      AuditLog.username(principal),
                      active ? "start" : "stop",
                      sanitize(extension),
                      sanitize(version));
        extensionController.updateExtensionStatus(new ExtensionKey(extension, version), context, active);
    }

    @DeleteMapping("context")
    public void unregisterExtensionForContext(@RequestParam String extension,
                                              @RequestParam String version,
                                              @RequestParam(required = false, name = "targetContext") String context,
                                              @ApiIgnore Principal principal) {
        if (!extensionsEnabled) {
            throw new MessagingPlatformException(ErrorCode.EXTENSIONS_DISABLED, EXTENSIONS_DISABLED);
        }
        auditLog.info("[{}] Request to unregister extension {}/{} for context {}.",
                      AuditLog.username(principal),
                      sanitize(extension),
                      sanitize(version),
                      sanitize(context));
        extensionController.unregisterExtensionForContext(new ExtensionKey(extension, version), context);
    }


    @GetMapping("configuration")
    public Iterable<ExtensionPropertyGroup> configuration(@RequestParam String extension,
                                                          @RequestParam String version,
                                                          @RequestParam(required = false, name = "targetContext") String context,
                                                          @ApiIgnore Principal principal) {
        if (!extensionsEnabled) {
            throw new MessagingPlatformException(ErrorCode.EXTENSIONS_DISABLED, EXTENSIONS_DISABLED);
        }
        auditLog.info("[{}] Request for configuration of {}/{}. ", AuditLog.username(principal),
                      sanitize(extension),
                      sanitize(version));
        return extensionController.listProperties(new ExtensionKey(extension, version), context);
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ExtensionKey installExtension(@RequestPart("bundle") MultipartFile extensionBundle,
                                         @ApiIgnore Principal principal)
            throws IOException {
        if (!extensionsEnabled) {
            throw new MessagingPlatformException(ErrorCode.EXTENSIONS_DISABLED, EXTENSIONS_DISABLED);
        }
        auditLog.info("[{}] Request to install extension {}. ",
                      AuditLog.username(principal),
                      sanitize(extensionBundle.getOriginalFilename()));

        try (InputStream inputStream = extensionBundle.getInputStream()) {
            String effectiveFilename = uniqueName(extensionBundle.getOriginalFilename());
            return extensionController.addExtension(effectiveFilename, inputStream);
        }
    }

    private String uniqueName(String originalFilename) {
        if (originalFilename == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 "No extension package provided");
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
    public void updateConfiguration(@RequestBody ExtensionConfigurationJSON configurationJSON,
                                    @ApiIgnore Principal principal) {
        if (!extensionsEnabled) {
            throw new MessagingPlatformException(ErrorCode.EXTENSIONS_DISABLED, EXTENSIONS_DISABLED);
        }
        auditLog.info("[{}] Request to update configuration of {}/{}. ", AuditLog.username(principal),
                      sanitize(configurationJSON.getExtension()),
                      sanitize(configurationJSON.getVersion()));
        extensionController.updateConfiguration(new ExtensionKey(configurationJSON.getExtension(),
                                                                 configurationJSON.getVersion()),
                                                configurationJSON.getContext(),
                                                configurationJSON.getProperties());
    }
}
