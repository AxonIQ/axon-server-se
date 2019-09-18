package io.axoniq.axonserver.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;

public class AuditLog {

    public static <T> Logger getLogger() {
        Class<?> clazz[] = CallingClass.INSTANCE.getCallingClasses();

        return LoggerFactory.getLogger("AUDIT." + clazz[2].getName());
    }

    public static String enablement(final boolean onOff) {
        return onOff ? "ENABLED" : "DISABLED";
    }

    public static String username(Principal principal) {
        return (principal == null) ? "<anonymous>" : principal.getName();
    }

    private static class CallingClass extends SecurityManager {
        static final CallingClass INSTANCE = new CallingClass();

        public Class[] getCallingClasses() {
            return getClassContext();
        }
    }
}
