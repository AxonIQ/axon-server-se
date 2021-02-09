/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.AxonServerStandardAccessController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.rest.WebSecurityConfigurer.TokenAuthenticationFilter;
import org.junit.*;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class WebSecurityConfigurationTest {
    private TokenAuthenticationFilter testSubject;
    private AtomicInteger statusCodeHolder = new AtomicInteger();
    private ServletResponse response = new MockHttpServletResponse(){
        @Override
        public void setStatus(int status) {
            statusCodeHolder.set(status);
        }
    };

    @Before
    public void setUp() {
        MessagingPlatformConfiguration messagingPlatformConfiguration = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 0;
            }

            @Override
            public String getHostName() throws UnknownHostException {
                return null;
            }
        });
        messagingPlatformConfiguration.getAccesscontrol().setToken("123456");
        AxonServerAccessController accessController = new AxonServerStandardAccessController(
                messagingPlatformConfiguration);
        testSubject = new TokenAuthenticationFilter(accessController);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void filterValidToken() throws IOException, ServletException {
        ServletRequest request = new MockHttpServletRequest() {
            @Override
            public String getHeader(String name) {
                if( name.equals(AxonServerAccessController.TOKEN_PARAM)) {
                    return "123456";
                }
                return super.getHeader(name);
            }
        };
        AtomicReference<Authentication> authentication = new AtomicReference<>();
        FilterChain filterChain = (servletRequest, servletResponse) ->
                authentication.set(SecurityContextHolder.getContext()
                                                        .getAuthentication());
        testSubject.doFilter(request, response, filterChain);
        assertNotNull(authentication.get());

        assertEquals(0, authentication.get().getAuthorities().size());
    }

    @Test
    public void filterValidTokenParameter() throws IOException, ServletException {
        ServletRequest request = new MockHttpServletRequest() {
            @Override
            public String getParameter(String name) {
                if( name.equals(AxonServerAccessController.TOKEN_PARAM)) {
                    return "123456";
                }
                return super.getParameter(name);
            }
        };

        AtomicReference<Authentication> authentication = new AtomicReference<>();
        FilterChain filterChain = (servletRequest, servletResponse) -> authentication.set(SecurityContextHolder.getContext()
                                                                                               .getAuthentication());
        testSubject.doFilter(request, response, filterChain);
        assertNotNull(authentication.get());
        assertEquals(0, authentication.get().getAuthorities().size());
    }

    @Test
    public void filterInvalidToken() throws IOException, ServletException {
        ServletRequest request = new MockHttpServletRequest() {
            @Override
            public String getHeader(String name) {
                if( name.equals(AxonServerAccessController.TOKEN_PARAM)) {
                    return "1234567";
                }
                return super.getHeader(name);
            }
        };
        AtomicReference<Authentication> authentication = new AtomicReference<>();
        FilterChain filterChain = (servletRequest, servletResponse) ->
                authentication.set(SecurityContextHolder.getContext()
                                                        .getAuthentication());

        testSubject.doFilter(request, response, filterChain);
        assertNull(authentication.get());
        assertEquals(403, statusCodeHolder.get());
    }

    @Test
    public void filterInvalidTokenLocalRequest() throws IOException, ServletException {
        ServletRequest request = new MockHttpServletRequest() {
            @Override
            public String getHeader(String name) {
                if( name.equals(AxonServerAccessController.TOKEN_PARAM)) {
                    return "1234567";
                }
                return super.getHeader(name);
            }

            @Override
            public String getLocalAddr() {
                return "localhost";
            }

            @Override
            public String getRemoteAddr() {
                return "localhost";
            }

            @Override
            public String getRequestURI() {
                return "/v1/context";
            }
        };
        AtomicReference<Authentication> authentication = new AtomicReference<>();
        FilterChain filterChain = (servletRequest, servletResponse) ->
                authentication.set(SecurityContextHolder.getContext()
                                                        .getAuthentication());

        testSubject.doFilter(request, response, filterChain);
        assertNull(authentication.get());
        assertEquals(403, statusCodeHolder.get());
    }

}