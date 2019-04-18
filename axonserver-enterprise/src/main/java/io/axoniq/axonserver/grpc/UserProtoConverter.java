package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.grpc.internal.User;

import java.util.stream.Collectors;

/**
 * Convert between JPA {@link io.axoniq.axonserver.access.jpa.User} object and Protobuf {@link User} object and vice versa.
 * @author Marc Gathier
 */
public class UserProtoConverter {
    public static io.axoniq.axonserver.access.jpa.User createJpaUser(User user) {
        return new io.axoniq.axonserver.access.jpa.User(user.getName(),
                                                user.getPassword(),
                                                user.getRolesList().toArray(new String[0]));
    }

    public static User createUser(io.axoniq.axonserver.access.jpa.User user) {
        return User.newBuilder()
                   .setName(user.getUserName())
                   .setPassword(user.getPassword() == null ? "" : user.getPassword())
                   .addAllRoles(user.getRoles()
                                    .stream()
                                    .map(UserRole::getRole)
                                    .collect(Collectors.toSet()))
                   .build();
    }

}
