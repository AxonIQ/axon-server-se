package io.axoniq.axonserver.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.security.authentication.event.AbstractAuthenticationEvent;
import org.springframework.security.authentication.event.AbstractAuthenticationFailureEvent;
import org.springframework.security.authentication.event.AuthenticationSuccessEvent;
import org.springframework.security.authentication.event.InteractiveAuthenticationSuccessEvent;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import java.security.Principal;

import static io.axoniq.axonserver.util.StringUtils.sanitize;

@Component
public class AuditLog implements ApplicationListener<AbstractAuthenticationEvent> {

    public static <T> Logger getLogger() {
        Class<?> clazz[] = CallingClass.INSTANCE.getCallingClasses();

        return LoggerFactory.getLogger("AUDIT." + clazz[2].getName());
    }

    public static String enablement(final boolean onOff) {
        return onOff ? "ENABLED" : "DISABLED";
    }

    public static String username(Principal principal) {
        return (principal == null) ? "<anonymous>" : sanitize(principal.getName());
    }

    private static final Logger auditLog = getLogger();

    @Override
    public void onApplicationEvent(AbstractAuthenticationEvent evt) {
        if (evt instanceof InteractiveAuthenticationSuccessEvent) {
            // ignores to prevent duplicate logging with AuthenticationSuccessEvent
            return;
        }
        Authentication authentication = evt.getAuthentication();
        if ((evt instanceof AuthenticationSuccessEvent) && authentication.isAuthenticated()) {
            auditLog.info("Login with username \"{}\".", authentication.getName());
        } else if (evt instanceof AbstractAuthenticationFailureEvent) {
            AbstractAuthenticationFailureEvent failure = (AbstractAuthenticationFailureEvent) evt;
            auditLog.error("Login with username \"{}\" FAILED: {}",
                           authentication.getName(),
                           failure.getException().getMessage());
        } else {
            auditLog.debug("Authentication event: {}", evt.getClass().getSimpleName());
        }
    }

    private static class CallingClass extends SecurityManager {

        static final CallingClass INSTANCE = new CallingClass();

        public Class[] getCallingClasses() {
            return getClassContext();
        }
    }
}
