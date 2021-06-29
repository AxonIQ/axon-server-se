/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.version.VersionInfo;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.junit.*;
import org.springframework.web.servlet.ModelAndView;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class LoginControllerTest {

    private final ExternalLoginsProvider externalLoginsProvider =
            () -> Collections.singletonList(new ExternalLogin("imageClass", "url", "name"));
    private final VersionInfoProvider versionInfoProvider =
            () -> new VersionInfo("theProduct", "theVersion");
    private final LoginController testSubject = new LoginController(versionInfoProvider, externalLoginsProvider);

    @Test
    public void loginError() {
        ModelAndView modelAndView = testSubject.login("there was an error", null, null);
        assertNotNull(modelAndView.getModel().get("error"));
        assertNull(modelAndView.getModel().get("logout"));
    }

    @Test
    public void loginNoAccount() {
        ModelAndView modelAndView = testSubject.login(null, "no account for user", null);
        assertNotNull(modelAndView.getModel().get("error"));
        assertNull(modelAndView.getModel().get("logout"));
    }

    @Test
    public void loginLogout() {
        ModelAndView modelAndView = testSubject.login(null, null, "logged out");
        assertNull(modelAndView.getModel().get("error"));
        assertNotNull(modelAndView.getModel().get("logout"));
    }

    @Test
    public void login() {
        ModelAndView modelAndView = testSubject.login(null, null, null);
        assertNull(modelAndView.getModel().get("error"));
        assertNull(modelAndView.getModel().get("logout"));
        assertEquals("theVersion", modelAndView.getModel().get("asVersion"));
        assertEquals("theProduct", modelAndView.getModel().get("product"));
        assertNotNull(modelAndView.getModel().get("externalLogins"));
        Collection<ExternalLogin> externalLogins = (Collection<ExternalLogin>) modelAndView.getModel().get(
                "externalLogins");
        assertEquals(1, externalLogins.size());
    }
}