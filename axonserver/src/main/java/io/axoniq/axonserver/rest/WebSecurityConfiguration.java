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
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.password.PasswordEncoder;

import javax.sql.DataSource;
import java.lang.invoke.MethodHandles;

/**
 * The Spring Security global configuration class. The {@link WebSecurityConfigurerAdapter} based configurer may be
 * plugged in here using {@link #setAltConfigurer(WebSecurityConfigurerAdapter)} to override or extend the default
 * settings. Note that if you don't use the existing one as base, you'll have to provide settings for the tokens.
 *
 * @author Marc Gathier
 */
@Configuration
@EnableWebSecurity
public class WebSecurityConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * The access-control part of the settings.
     */
    private final AccessControlConfiguration accessControlConfiguration;

    /**
     * The {@link AxonServerAccessController} that will decide if access is allowed.
     */
    private final AxonServerAccessController accessController;

    /**
     * The data-source for the username/password checking. (if access-control is enabled)
     */
    private final DataSource dataSource;

    /**
     * An optional alternate {@link WebSecurityConfigurerAdapter}.
     */
    private WebSecurityConfigurerAdapter altConfigurer;

    public WebSecurityConfiguration(MessagingPlatformConfiguration conf,
                                    AxonServerAccessController accessController,
                                    DataSource dataSource) {
        this.accessControlConfiguration = conf.getAccesscontrol();
        this.accessController = accessController;

        this.dataSource = dataSource;
    }

    /**
     * Initialize username-password authentication if access-control is enabled.
     *
     * @param auth            the {@link AuthenticationManagerBuilder} to be used.
     * @param passwordEncoder the {@link PasswordEncoder} to use.
     * @throws Exception if configuration fails.
     */
    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth, PasswordEncoder passwordEncoder) throws Exception {
        if (accessControlConfiguration.isEnabled()) {
            auth.jdbcAuthentication()
                .dataSource(dataSource)
                .usersByUsernameQuery(accessController.usersByUsernameQuery())
                .authoritiesByUsernameQuery(accessController.authoritiesByUsernameQuery())
                .passwordEncoder(passwordEncoder);
        }
    }

    /**
     * Set an alternate {@link WebSecurityConfigurerAdapter}.
     *
     * @param altConfigurer the configurer to use.
     */
    public void setAltConfigurer(WebSecurityConfigurerAdapter altConfigurer) {
        logger.info("Setting up alternate security configurer. ({})", altConfigurer.getClass().getName());
        this.altConfigurer = altConfigurer;
    }

    /**
     * Producer for the configurer to use.
     *
     * @return the configurer to use.
     */
    @Bean
    public WebSecurityConfigurerAdapter webSecurityConfigurer() {
        if (altConfigurer == null) {
            logger.debug("Using default security configurer.");
            return new WebSecurityConfigurer(accessControlConfiguration, accessController);
        } else {
            logger.debug("Using alternate security configurer.");
            return altConfigurer;
        }
    }
}
