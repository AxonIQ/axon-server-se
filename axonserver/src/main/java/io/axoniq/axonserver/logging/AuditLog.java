package io.axoniq.axonserver.logging;

import io.axoniq.axonserver.applicationevents.UserEvents.UserDeleted;
import io.axoniq.axonserver.applicationevents.UserEvents.UserUpdated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.security.authentication.event.AbstractAuthenticationEvent;
import org.springframework.security.authentication.event.AbstractAuthenticationFailureEvent;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.security.authentication.event.InteractiveAuthenticationSuccessEvent;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.security.Principal;

import static io.axoniq.axonserver.util.StringUtils.sanitize;

@Component
public class AuditLog {

    public static <T> Logger getLogger() {
        Class<?> clazz[] = CallingClass.INSTANCE.getCallingClasses();

        return LoggerFactory.getLogger("AUDIT." + clazz[2].getName());
    }

    public static String enablement(final boolean onOff) {
        return onOff ? "ENABLED" : "DISABLED";
    }

    public static String username(String username) {
        return (username == null) ? "<anonymous>" : sanitize(username);
    }

    public static String username(Principal principal) {
        return (principal == null) ? "<anonymous>" : sanitize(principal.getName());
    }

    private static final Logger auditLog = getLogger();

    @EventListener
    public void on(AbstractAuthenticationEvent event) {
        if (event instanceof InteractiveAuthenticationSuccessEvent) {
            // ignores to prevent duplicate logging with AuthenticationSuccessEvent
            return;
        }
        Authentication authentication = event.getAuthentication();
        if (auditLog.isDebugEnabled()) {
            auditLog.debug(event.toString());
        } else if ((event instanceof AuthenticationSuccessEvent) && authentication.isAuthenticated()) {
            auditLog.info("Login with username \"{}\".", authentication.getName());
        } else if (event instanceof AbstractAuthenticationFailureEvent) {
            AbstractAuthenticationFailureEvent failure = (AbstractAuthenticationFailureEvent) event;
            auditLog.error("Login with username \"{}\" FAILED: {}",
                           authentication.getName(),
                           failure.getException().getMessage());
        }
    }

    @EventListener
    public void on(UserUpdated event) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug(event.toString());
        }
    }

    @EventListener
    public void on(UserDeleted event) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug(event.toString());
        }
    }

    private static class CallingClass extends SecurityManager {

        static final CallingClass INSTANCE = new CallingClass();

        public Class[] getCallingClasses() {
            return getClassContext();
        }
    }
}
