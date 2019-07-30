package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.access.application.JpaContextUser;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.axonserver.grpc.internal.UserContextRole;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.util.StringUtils.getOrDefault;

/**
 * Convert between JPA {@link io.axoniq.axonserver.access.jpa.User} object and Protobuf {@link User} object and vice versa.
 * @author Marc Gathier
 */
public class UserProtoConverter {
    public static io.axoniq.axonserver.access.jpa.User createJpaUser(User user) {
        return new io.axoniq.axonserver.access.jpa.User(user.getName(),
                                                        user.getPassword(),
                                                        asUserRoles(user.getRolesList()));
    }

    private static Set<UserRole> asUserRoles(List<UserContextRole> rolesList) {
        return rolesList.stream()
                        .map(userContextRole -> new UserRole(userContextRole.getContext(), userContextRole.getRole())
                        ).collect(Collectors.toSet());
    }

    public static User createUser(io.axoniq.axonserver.access.jpa.User user) {
        return User.newBuilder()
                   .setName(user.getUserName())
                   .setPassword(user.getPassword() == null ? "" : user.getPassword())
                   .addAllRoles(user.getRoles()
                                    .stream()
                                    .map(userRole -> UserContextRole.newBuilder()
                                                                    .setRole(userRole.getRole())
                                                                    .setContext(getOrDefault(userRole.getContext(),
                                                                                             "*"))
                                                                    .build())
                                    .collect(Collectors.toSet()))
                   .build();
    }

    public static ContextUser createContextUser(JpaContextUser user) {
        return ContextUser.newBuilder().setContext(user.getContext())
                          .setUser(User.newBuilder()
                                       .setName(user.getUsername())
                                       .setPassword(getOrDefault(user.getPassword(), ""))
                                       .addAllRoles(user.getRoles()
                                                        .stream()
                                                        .map(userRole -> UserContextRole.newBuilder()
                                                                                        .setRole(userRole)
                                                                                        .build())
                                                        .collect(Collectors.toSet()))
                          ).build();
    }

    public static JpaContextUser createJpaContextUser(ContextUser userMessage) {
        JpaContextUser jpaContextUser = new JpaContextUser(userMessage.getContext(), userMessage.getUser().getName());
        jpaContextUser.setPassword(userMessage.getUser().getPassword());
        jpaContextUser.setRoles(userMessage.getUser()
                                           .getRolesList()
                                           .stream()
                                           .map(UserContextRole::getRole)
                                           .collect(Collectors.toSet()));

        return jpaContextUser;
    }

    public static ContextUser createContextUser(String context, io.axoniq.axonserver.access.jpa.User app) {
        return ContextUser.newBuilder().setContext(context)
                          .setUser(createUser(app)).build();
    }
}
