package io.axoniq.axonserver.refactoring.transport.rest;

import io.axoniq.axonserver.refactoring.api.Authentication;
import org.springframework.security.core.GrantedAuthority;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Sara Pellegrini
 * @since
 */
public class SpringAuthentication implements Authentication {

    private final org.springframework.security.core.Authentication springAuthentication;
    private final Map<String, String> detailsMap;

    public SpringAuthentication(org.springframework.security.core.Authentication springAuthentication) {
        this.springAuthentication = springAuthentication;
        if (springAuthentication == null || !(springAuthentication.getDetails() instanceof Map)) {
            this.detailsMap = Collections.emptyMap();
        } else {
            this.detailsMap = (Map<String, String>) springAuthentication.getDetails();
        }
    }

    @Override
    public String name() {
        return springAuthentication.getName();
    }

    @Override
    public Set<String> roles() {
        return springAuthentication.getAuthorities()
                                   .stream()
                                   .map(GrantedAuthority::getAuthority)
                                   .collect(Collectors.toSet());
    }

    @Override
    public String detail(String key) {
        return detailsMap.get(key);
    }

    @Override
    public Set<String> detailsKeys() {
        return detailsMap.keySet();
    }
}
