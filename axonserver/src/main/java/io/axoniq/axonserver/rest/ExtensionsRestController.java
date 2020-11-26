/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.BundleInfo;
import io.axoniq.axonserver.config.OsgiController;
import org.osgi.framework.BundleException;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import springfox.documentation.annotations.ApiIgnore;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;

/**
 * @author Marc Gathier
 */
@RestController
@RequestMapping("v1/extensions")
public class ExtensionsRestController {

    private final OsgiController osgiController;

    public ExtensionsRestController(OsgiController osgiController) {
        this.osgiController = osgiController;
    }

    @GetMapping
    public Iterable<BundleInfo> currentExtensions(@ApiIgnore Principal principal) {
        return osgiController.listBundles();
    }

    @DeleteMapping
    public void uninstallExtension(@RequestParam long id, @ApiIgnore Principal principal) throws BundleException {
        osgiController.uninstallExtension(id);
    }

    @PostMapping
    public void installExtension(@RequestParam("bundle") MultipartFile extensionBundle, @ApiIgnore Principal principal)
            throws IOException, BundleException {
        try (InputStream inputStream = extensionBundle.getInputStream()) {
            osgiController.addBundle(extensionBundle.getOriginalFilename(), inputStream);
        }
    }
}
