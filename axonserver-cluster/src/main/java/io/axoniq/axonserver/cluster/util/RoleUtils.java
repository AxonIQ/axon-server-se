package io.axoniq.axonserver.cluster.util;

import io.axoniq.axonserver.grpc.cluster.Role;

/**
 * Utility functions for working with roles.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class RoleUtils {

    private static final Role DEFAULT_ROLE = Role.PRIMARY;

    /**
     * Returns the number for the role. If role is null it returns the number for the default role (PRIMARY).
     *
     * @param role the role
     * @return the number for the role
     */
    public static int getNumber(Role role) {
        return role == null ? DEFAULT_ROLE.getNumber() : role.getNumber();
    }

    /**
     * Returns the role for the number. If role is null it returns the default role (PRIMARY).
     *
     * @param roleNumber the role number
     * @return the role
     */
    public static Role forNumber(Integer roleNumber) {
        return roleNumber == null ? DEFAULT_ROLE : Role.forNumber(roleNumber);
    }

    /**
     * Returns if role is a context member role that allows clients to connect.
     *
     * @param role the role
     * @return true if a client can connect to a node with this role
     */
    public static boolean allowsClientConnect(Role role) {
        return Role.PRIMARY.equals(role) || Role.MESSAGING_ONLY.equals(role);
    }
}
