package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.AxonServerStandardAccessController;
import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.ApplicationContext;
import io.axoniq.axonserver.access.jpa.ApplicationContextRole;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.ExpressionUrlAuthorizationConfigurer;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.web.filter.GenericFilterBean;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
            ExpressionUrlAuthorizationConfigurer<HttpSecurity>.ExpressionInterceptUrlRegistry auth = http
                    .authorizeRequests()
                    .antMatchers("/", "/**/*.html", "/v1/public/*", "/v1/public", "/v1/search").authenticated();

            accessController.getPathMappings().stream().filter(p -> p.getPath().contains(":"))
                            .forEach(p -> {
                                String[] parts = p.getPath().split(":");
                                if (accessController.isRoleBasedAuthentication()) {
                                    auth.antMatchers(HttpMethod.valueOf(parts[0]), parts[1]).hasAuthority(p.getRole());
                                } else {
                                    auth.antMatchers(HttpMethod.valueOf(parts[0]), parts[1]).authenticated();
                                }
                            });
            auth
                    .anyRequest().permitAll()
                    .and()
                    .formLogin()
                    .loginPage("/login")
                    .permitAll()
                    .and()
                    .logout()
                    .permitAll();
        } else {
            http.authorizeRequests().anyRequest().permitAll();
        }
    }

    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth, PasswordEncoder passwordEncoder) throws Exception {
        if (accessControlConfiguration.isEnabled()) {
            auth.jdbcAuthentication()
                .dataSource(dataSource)
                .usersByUsernameQuery("select username,password, enabled from users where username=?")
                .authoritiesByUsernameQuery("select username, role from user_roles where username=?")
                .passwordEncoder(passwordEncoder);
        }
    }

    public static class AuthenticationToken implements Authentication {

        private final boolean authenticated;
        private final String name;
        private final Set<String> roles;

        AuthenticationToken(boolean authenticated, String name, Set<String> roles) {
            this.authenticated = authenticated;
            this.name = name;
            this.roles = roles;
        }

        @Override
        public Collection<? extends GrantedAuthority> getAuthorities() {
            return roles.stream()
                        .map(s -> (GrantedAuthority) () -> s)
                        .collect(Collectors.toSet());
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
        public void setAuthenticated(boolean b)  {
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

                if( actuatorRequest(request)) {
                    // No further action
                } else if ( isLocalRequest(request)) {
                    SecurityContextHolder.getContext().setAuthentication(
                            new AuthenticationToken(true,
                                                    "LocalAdmin",
                                                    Collections.singleton("ADMIN")));
                } else {
                    if (token != null) {
                        Application application = accessController.getApplication(token);
                        if (application != null) {
                            Set<String> roles = application.getContexts()
                                                           .stream()
                                                           .map(ApplicationContext::getRoles)
                                                           .flatMap(List::stream)
// TODO: filter?
//                                                           .filter(r -> Topology.DEFAULT_CONTEXT.equals(r.getApplicationContext().getContext()))
                                                           .map(ApplicationContextRole::getRole)
                                                           .collect(Collectors.toSet());
                            SecurityContextHolder.getContext().setAuthentication(
                                    new AuthenticationToken(true,
                                                            application.getName(),
                                                            roles));
                        } else {
                            HttpServletResponse httpServletResponse = (HttpServletResponse)servletResponse;
                            httpServletResponse.setStatus(ErrorCode.AUTHENTICATION_INVALID_TOKEN.getHttpCode().value());
                            try (ServletOutputStream outputStream = httpServletResponse.getOutputStream() ) {
                                outputStream.println("Invalid token");
                            }
                            return;
                        }
                    } else if( stopRedirect(request.getHeader(HttpHeaders.USER_AGENT))) {
                        HttpServletResponse httpServletResponse = (HttpServletResponse)servletResponse;
                        httpServletResponse.setStatus(ErrorCode.AUTHENTICATION_TOKEN_MISSING.getHttpCode().value());
                        try (ServletOutputStream outputStream = httpServletResponse.getOutputStream() ) {
                            outputStream.println("Missing header: " + AxonServerStandardAccessController.TOKEN_PARAM);
                        }
                        return;
                    }
                }
            }
            filterChain.doFilter(servletRequest, servletResponse);
        }

        private boolean actuatorRequest(HttpServletRequest httpServletRequest) {
            return httpServletRequest.getRequestURI().startsWith("/actuator/");
        }

        private boolean stopRedirect(String header) {
            if( header == null) return false;
            String lowercaseHeader = header.toLowerCase();
            return lowercaseHeader.startsWith("apache-httpclient") || lowercaseHeader.startsWith("curl")
                    || lowercaseHeader.startsWith("wget");
        }

        private boolean isLocalRequest(HttpServletRequest httpServletRequest) {
            return ( httpServletRequest.getRequestURI().startsWith("/v1/cluster") ||
                    httpServletRequest.getRequestURI().startsWith("/v1/context") ||
                    httpServletRequest.getRequestURI().startsWith("/v1/users") ||
                    httpServletRequest.getRequestURI().startsWith("/v1/applications") )
                    && httpServletRequest.getLocalAddr().equals(httpServletRequest.getRemoteAddr());
        }
    }
}
