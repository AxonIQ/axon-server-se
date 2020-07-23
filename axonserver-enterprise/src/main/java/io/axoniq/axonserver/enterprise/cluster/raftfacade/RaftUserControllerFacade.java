package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.access.user.UserControllerFacade;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.grpc.internal.UserContextRole;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class RaftUserControllerFacade implements UserControllerFacade {

    private final UserController userController;
    private final PasswordEncoder passwordEncoder;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RaftUserControllerFacade(UserController userController, PasswordEncoder passwordEncoder,
                                    RaftConfigServiceFactory raftServiceFactory) {
        this.userController = userController;
        this.passwordEncoder = passwordEncoder;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void updateUser(String userName, String password, Set<UserRole> roles) {
        if (!StringUtils.isEmpty(password)) {
            password = passwordEncoder.encode(password);
        }
        raftServiceFactory.getRaftConfigService()
                          .updateUser(io.axoniq.axonserver.grpc.internal.User
                                              .newBuilder()
                                              .setName(userName)
                                              .setPassword(
                                                                             StringUtils
                                                                                     .getOrDefault(
                                                                                             password,
                                                                                             ""))
                                              .addAllRoles(map(roles))
                                              .build());
    }

    private Iterable<? extends UserContextRole> map(Set<UserRole> roles) {
        List<UserContextRole> userRoles = new ArrayList<>();
        roles.forEach(r -> userRoles.add(UserContextRole.newBuilder()
                                                        .setContext(r.getContext())
                                                        .setRole(r.getRole())
                                                        .build()));
        return userRoles;
    }

    @Override
    public List<User> getUsers() {
        return userController.getUsers();
    }

    @Override
    public void deleteUser(String name) {
        raftServiceFactory.getRaftConfigService().deleteUser(io.axoniq.axonserver.grpc.internal.User.newBuilder()
                                                                                                    .setName(name)
                                                                                                    .build());
    }
}
