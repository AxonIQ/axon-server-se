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
import io.axoniq.axonserver.exception.ErrorCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.vote.AffirmativeBased;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.ExpressionUrlAuthorizationConfigurer;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.filter.GenericFilterBean;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

/**
 * @author Marc Gathier
 */
@Configuration
@EnableWebSecurity
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {

    private final AccessControlConfiguration accessControlConfiguration;
    private final AxonServerAccessController accessController;

    private final DataSource dataSource;

    public WebSecurityConfiguration(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                    AxonServerAccessController accessController, DataSource dataSource) {
        this.accessControlConfiguration = messagingPlatformConfiguration.getAccesscontrol();
        this.accessController = accessController;
        this.dataSource = dataSource;
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.headers().frameOptions().disable();
        if (accessControlConfiguration.isEnabled()) {
            final TokenAuthenticationFilter tokenFilter = new TokenAuthenticationFilter(accessController);
            http.addFilterBefore(tokenFilter, BasicAuthenticationFilter.class);
            http.exceptionHandling()
                .accessDeniedHandler(this::handleAccessDenied)
                // only redirect to login page for html pages
                .defaultAuthenticationEntryPointFor(new LoginUrlAuthenticationEntryPoint("/login"),
                                                    new AntPathRequestMatcher("/**/*.html"));
            ExpressionUrlAuthorizationConfigurer<HttpSecurity>.ExpressionInterceptUrlRegistry auth = http
                    .authorizeRequests();

            if (accessController.isRoleBasedAuthentication()) {
                auth.antMatchers("/", "/**/*.html", "/v1/**")
                    .authenticated()
                    .accessDecisionManager(new AffirmativeBased(
                            Collections.singletonList(
                                    new RestRequestAccessDecisionVoter(accessController))));
            } else {
                auth.antMatchers("/", "/**/*.html", "/v1/**")
                    .authenticated();
            }
            auth
                    .anyRequest().permitAll()
                    .and()
                    .formLogin()
                    .loginPage("/login")
                    .permitAll()
                    .and()
                    .logout()
                    .permitAll()
                    .and()
                    .httpBasic(); // Allow accessing rest calls using basic authentication header
        } else {
            http.authorizeRequests().anyRequest().permitAll();
        }
    }

    private void handleAccessDenied(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
                                    AccessDeniedException e) throws IOException {
        if (MediaType.TEXT_EVENT_STREAM_VALUE.equals(httpServletRequest.getHeader(HttpHeaders.ACCEPT))) {
            httpServletResponse.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE);
            ServletOutputStream os = httpServletResponse.getOutputStream();
            os.println("event:error");
            os.println("data:" + HttpStatus.FORBIDDEN.getReasonPhrase());
            os.println();
            os.close();
        } else {
            httpServletResponse.sendError(HttpStatus.FORBIDDEN.value(), HttpStatus.FORBIDDEN.getReasonPhrase());
        }
    }

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

    public static class AuthenticationToken implements Authentication {

        private final boolean authenticated;
        private final String name;
        private final Set<GrantedAuthority> roles;

        AuthenticationToken(boolean authenticated, String name, Set<String> roles) {
            this.authenticated = authenticated;
            this.name = name;
            this.roles = roles.stream()
                              .map(s -> (GrantedAuthority) () -> s)
                              .collect(Collectors.toSet());
        }

        @Override
        public Collection<? extends GrantedAuthority> getAuthorities() {
            return roles;
        }

        @Override
        public Object getCredentials() {
            return null;
        }

        @Override
        public Object getDetails() {
            return null;
        }

        @Override
        public Object getPrincipal() {
            return "Connected Application";
        }

        @Override
        public boolean isAuthenticated() {
            return authenticated;
        }

        @Override
        public void setAuthenticated(boolean b) {
            // authenticated is only set in constructor
        }

        @Override
        public String getName() {
            return name;
        }
    }

    public static class TokenAuthenticationFilter extends GenericFilterBean {

        private final AxonServerAccessController accessController;

        public TokenAuthenticationFilter(AxonServerAccessController accessController) {
            this.accessController = accessController;
        }

        @Override
        public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
                throws IOException, ServletException {
            if (SecurityContextHolder.getContext().getAuthentication() == null) {
                final HttpServletRequest request = (HttpServletRequest) servletRequest;
                String token = request.getHeader(AxonServerAccessController.TOKEN_PARAM);
                if (token == null) {
                    token = request.getParameter(AxonServerAccessController.TOKEN_PARAM);
                }

                if (token != null) {
                    Set<String> roles = accessController.getRoles(token);
                    if (roles != null) {
                        SecurityContextHolder.getContext().setAuthentication(
                                new AuthenticationToken(true,
                                                        "AuthenticatedApp",
                                                        roles));
                    } else {
                        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
                        httpServletResponse.setStatus(ErrorCode.AUTHENTICATION_INVALID_TOKEN.getHttpCode().value());
                        try (ServletOutputStream outputStream = httpServletResponse.getOutputStream()) {
                            outputStream.println("Invalid token");
                        }
                        return;
                    }
                }
            }
            try {
                filterChain.doFilter(servletRequest, servletResponse);
            } finally {
                if (SecurityContextHolder.getContext().getAuthentication() instanceof AuthenticationToken) {
                    SecurityContextHolder.getContext().setAuthentication(null);
                }
            }
        }
    }
}
