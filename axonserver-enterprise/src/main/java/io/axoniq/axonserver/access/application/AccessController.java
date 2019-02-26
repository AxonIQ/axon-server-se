package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.access.jpa.PathMapping;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import javax.annotation.PostConstruct;

/**
 * @author Marc Gathier
 * Access Controller for AxonServer Enterprise. Uses role based access control, app can have various roles.
 * Maintains a cache of checked commands for improved performance.
 */
@Primary
@Controller
public class AccessController implements AxonServerAccessController {

    private final AccessControllerDB accessControllerDB;
    private TimeLimitedCache<RequestWithToken, Boolean> cache;

    @Value("${axoniq.axonserver.accesscontrol.cache-ttl:300000}")
    private long timeToLive;

    private static class RequestWithToken {
        final String request;
        private final String context;
        final String token;

        private RequestWithToken(String request, String context, String token) {
            this.request = request;
            this.context = context;
            this.token = token;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RequestWithToken that = (RequestWithToken) o;
            return Objects.equals(request, that.request) &&
                    Objects.equals(context, that.context) &&
                    Objects.equals(token, that.token);
        }

        @Override
        public int hashCode() {
            return Objects.hash(request, context, token);
        }
    }

    public AccessController(AccessControllerDB accessControllerDB) {
        this.accessControllerDB = accessControllerDB;
    }

    @PostConstruct
    public void init() {
        cache = new TimeLimitedCache<>(timeToLive);
    }

    @Override
    public boolean allowed(String fullMethodName, String context, String token) {
        RequestWithToken requestWithToken = new RequestWithToken(fullMethodName,context,token);
        if( cache.get(requestWithToken) != null) {
            return true;
        }

        boolean authorized = accessControllerDB.authorize(token, context, fullMethodName, isRoleBasedAuthentication());
        if( authorized) {
            cache.put(requestWithToken, Boolean.TRUE);
        }
        return authorized;
    }

    @Override
    public Collection<PathMapping> getPathMappings() {
        return accessControllerDB.getPathMappings();
    }

    @Override
    public boolean isRoleBasedAuthentication() {
        return true;
    }

    public Set<String> getAdminRoles(String token) {
        return accessControllerDB.getAdminRoles(token);
    }

}
