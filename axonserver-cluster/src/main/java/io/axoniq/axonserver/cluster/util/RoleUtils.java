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
     * Returns if role is a role that participates in leader election. The node may be a primary node or a
     * active backup node.
     *
     * @param role the role
     * @return true if role is a role that participates in leader election
     */
    public static boolean votingNode(Role role) {
        return Role.PRIMARY.equals(role) || Role.ACTIVE_BACKUP.equals(role);
    }

    /**
     * Returns if role is a primary node role.
     * @param role the role
     * @return true if role is a primary node role
     */
    public static boolean primaryNode(Role role) {
        return Role.PRIMARY.equals(role);
    }

    /**
     * Checks if role represents a role that stores event data.
     *
     * @param role the role to check
     * @return true if role represents a role that stores event data
     */
    public static boolean hasStorage(Role role) {
        return !Role.MESSAGING_ONLY.equals(role);
    }
    /*
     * Returns role if it is not null. If it is null, returns the default role.
     *
     * @param role the role to check
     * @return the role or the default role
     */
    public static Role getOrDefault(Role role) {
        return role == null ? DEFAULT_ROLE : role;
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
