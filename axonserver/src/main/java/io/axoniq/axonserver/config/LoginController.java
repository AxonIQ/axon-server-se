/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.util.ObjectUtils;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import java.util.Collections;

/**
 * Controller for the login page. Adds information on Axon Server version and external logins to the view.
 *
 * @author Marc Gathier
 * @since 4.5
 */

@Controller
public class LoginController {

    private final VersionInfoProvider versionInfoProvider;
    private final ExternalLoginsProvider externalLoginProvider;

    /**
     * @param versionInfoProvider    provides Axon Server version info
     * @param externalLoginsProvider provides information on external login options
     */
    public LoginController(VersionInfoProvider versionInfoProvider,
                           ExternalLoginsProvider externalLoginsProvider) {
        this.versionInfoProvider = versionInfoProvider;
        this.externalLoginProvider = externalLoginsProvider;
    }

    @RequestMapping("/login")
    public ModelAndView login(@RequestParam(value = "error", required = false) String error,
                              @RequestParam(value = "no-account", required = false) String noaccount,
                              @RequestParam(value = "logout", required = false) String logout) {

        ModelAndView model = new ModelAndView();
        if (error != null) {
            model.addObject("error", "Invalid username and password.");
        }

        if (noaccount != null) {
            model.addObject("error", "You do not have the required roles for this application.");
        }

        if (logout != null) {
            model.addObject("logout", "You've been logged out successfully.");
        }
        model.addObject("asVersion", versionInfoProvider.get().getVersion());
        model.addObject("product", versionInfoProvider.get().getProductName());
        model.addObject("externalLogins",
                        ObjectUtils.getOrDefault(externalLoginProvider.get(), Collections.emptyList()));
        model.setViewName("login");

        return model;
    }
}
