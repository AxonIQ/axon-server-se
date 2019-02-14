package io.axoniq.axonserver;

/**
 * @author Marc Gathier
 *
 * Utility class to check if context is admin context and to retrieve admin group name
 */
public class RaftAdminGroup {
    private static final String ADMIN_GROUP = "_admin";

    public static boolean isAdmin(String context) {
        return ADMIN_GROUP.equals(context);
    }

    public static String getAdmin() {
        return ADMIN_GROUP;
    }
}
