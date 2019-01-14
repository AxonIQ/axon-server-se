package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.Application;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;
import java.util.Objects;

/**
 * @author Marc Gathier
 */
@Controller
public class AccessController {

    private final AccessControllerDB accessControllerDB;
    private TimeLimitedCache<RequestWithToken, Boolean> cache;

    @Value("${axoniq.platform.accesscontrol.cache-ttl:300000}")
    private long timeToLive;

    static class RequestWithToken {
        final String request;
        private final String context;
        final String token;

        public RequestWithToken(String request, String context, String token) {
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

    public boolean validToken(String token) {
        return accessControllerDB.validToken(token);
    }

    public Application getApplicationByToken(String token) {
        return accessControllerDB.getApplicationByToken(token);
    }

    public boolean authorize(String token, String context, String path, boolean fineGrainedAccessControl) {
        RequestWithToken requestWithToken = new RequestWithToken(path,context,token);
        if( cache.get(requestWithToken) != null) {
            return true;
        }

        boolean authorized = accessControllerDB.authorize(token, context, path, fineGrainedAccessControl);
        if( authorized) {
            cache.put(requestWithToken, Boolean.TRUE);
        }
        return authorized;
    }

}
