package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.InvalidTokenException;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.vote.AffirmativeBased;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.ExpressionUrlAuthorizationConfigurer;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The default {@link WebSecurityConfigurerAdapter} for Axon Server. This one configures the token filter, and sets
 * all rules for protecting (or not) the UI and REST API.
 *
 * @author Marc Gathier
 */
public class WebSecurityConfigurer extends WebSecurityConfigurerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * The access-control part of the Axon Server settings.
     */
    private final AccessControlConfiguration accessControlConfiguration;

    /**
     * The {@link AxonServerAccessController} that will decide if access is allowed.
     */
    private final AxonServerAccessController accessController;

    public WebSecurityConfigurer(AccessControlConfiguration accessControlConfiguration,
                                 AxonServerAccessController accessController) {
        this.accessControlConfiguration = accessControlConfiguration;
        this.accessController = accessController;
    }

    /**
     * Set up security on the REST API and UI.
     *
     * @param http the {@link HttpSecurity} object to configure.
     * @throws Exception if configuration fails.
     */
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        logger.debug("Configuring Web Security.");

        http.csrf().disable();
        http.headers().frameOptions().disable();
        if (accessControlConfiguration.isEnabled()) {
            logger.debug("Access control is ENABLED. Setting up filters and matchers.");

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
                auth.antMatchers("/", "/**/*.html", "/v1/**", "/v2/**", "/internal/**")
                        .authenticated()
                        .accessDecisionManager(new AffirmativeBased(
                                Collections.singletonList(
                                        new RestRequestAccessDecisionVoter(accessController))));
            } else {
                auth.antMatchers("/", "/**/*.html", "/v1/**", "/v2/**", "/internal/**")
                        .authenticated()
                        .accessDecisionManager(new AffirmativeBased(
                                Collections.singletonList(
                                        new StandardEditionRestRequestAccessDecisionVoter())));
            }
            auth
                    .anyRequest().permitAll()
                    .and()
                    .formLogin().loginPage("/login").permitAll()
                    .and()
                    .logout().permitAll()
                    .and()
                    .httpBasic(); // Allow accessing rest calls using basic authentication header
        } else {
            http.authorizeRequests().anyRequest().permitAll();
        }
    }

    /**
     * Handle {@link AccessDeniedException}s thrown during access checks.
     *
     * @param httpServletRequest  the request being processed.
     * @param httpServletResponse the response to this request.
     * @param e                   the {@link AccessDeniedException} thrown
     * @throws IOException if writing an event stream and adding the error message failed.
     */
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

        private static final Logger auditLog = AuditLog.getLogger();

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
                    try {
                        String context = request.getHeader(AxonServerAccessController.CONTEXT_PARAM);
                        if (context == null) {
                            context = request.getParameter(AxonServerAccessController.CONTEXT_PARAM);
                        }
                        if (context == null) {
                            context = request.getParameter("context");
                        }
                        if (context == null) {
                            context = accessController.defaultContextForRest();
                        }
                        Authentication authentication = accessController
                                .authentication(context, token);
                        auditLog.trace("Access using configured token.");
                        SecurityContextHolder.getContext().setAuthentication(authentication);
                    } catch (InvalidTokenException invalidTokenException) {
                        auditLog.error("Access attempted with invalid token.");
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
