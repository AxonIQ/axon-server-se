package io.axoniq.axonserver.grpc;


import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.grpc.internal.Action;
import io.axoniq.axonserver.grpc.internal.User;

import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class UserProtoConverter {

    public static User mergeUser(io.axoniq.axonserver.access.jpa.User user) {
        return User.newBuilder().setAction(Action.MERGE)
            .setName(user.getUserName())
            .setPassword(user.getPassword() == null? "" : user.getPassword())
            .addAllRoles(user.getRoles()
                             .stream()
                             .map(UserRole::getRole)
                             .collect(Collectors.toSet()))
            .build();
    }

    public static User deleteUser(String name) {
        return User.newBuilder().setAction(Action.DELETE)
                   .setName(name)
                   .build();
    }

    public static io.axoniq.axonserver.access.jpa.User createJpaUser(User user) {
        return new io.axoniq.axonserver.access.jpa.User(user.getName(), user.getPassword(), user.getRolesList().toArray(new String[0]));
    }
}
