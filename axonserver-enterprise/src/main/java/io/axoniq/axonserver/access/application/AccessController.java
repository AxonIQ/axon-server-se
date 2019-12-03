package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.AxonServerAccessController;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Controller;

import java.util.Objects;
import java.util.Set;
import javax.annotation.PostConstruct;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

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

        boolean authorized = accessControllerDB.authorize(token, context, fullMethodName);
        if( authorized) {
            cache.put(requestWithToken, Boolean.TRUE);
        }
        return authorized;
    }

    @Override
    public boolean isRoleBasedAuthentication() {
        return true;
    }

    public Set<String> getRoles(String token) {
        return accessControllerDB.getRoles(token);
    }

    @Override
    public Set<String> rolesForOperation(String permission) {
        return accessControllerDB.rolesForOperation(permission);
    }

    @Override
    public String defaultContextForRest() {
        return getAdmin();
    }

    @Override
    public String usersByUsernameQuery() {
        return "select distinct username,password, 1 from jpa_context_user where username=? and password is not null";
    }

    @Override
    public String authoritiesByUsernameQuery() {
        return "select jpa_context_user.username, concat(jpa_context_user_roles.roles, '@', jpa_context_user.context) "
                + "from jpa_context_user_roles,jpa_context_user "
                + "where jpa_context_user.username=? "
                + "and jpa_context_user_roles.jpa_context_user_id = jpa_context_user.id";
    }

}
