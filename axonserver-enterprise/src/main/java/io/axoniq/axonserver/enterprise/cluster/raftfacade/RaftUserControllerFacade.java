package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.rest.UserControllerFacade;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Author: marc
 */
public class RaftUserControllerFacade implements UserControllerFacade {

    private final UserController userController;
    private final PasswordEncoder passwordEncoder;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RaftUserControllerFacade(UserController userController, PasswordEncoder passwordEncoder, RaftConfigServiceFactory raftServiceFactory) {
        this.userController = userController;
        this.passwordEncoder = passwordEncoder;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void updateUser(String userName, String password, String[] roles) {
        try {
            if( ! StringUtils.isEmpty(password)) {
                password = passwordEncoder.encode(password);
            }
            raftServiceFactory.getRaftConfigService().updateUser(io.axoniq.axonserver.grpc.internal.User
                                                                                       .newBuilder()
                                                                                       .setName(userName)
                                                                                       .setPassword(
                                                                                               StringUtils
                                                                                                       .getOrDefault(
                                                                                                               password,
                                                                                                               ""))
                                                                                       .addAllRoles(Arrays.asList(roles))
                                                                                       .build()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Updating user interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Updating user failed", e.getCause());
        }
    }

    @Override
    public List<User> getUsers() {
        return userController.getUsers();
    }

    @Override
    public void deleteUser(String name) {
        try {
            raftServiceFactory.getRaftConfigService().deleteUser(io.axoniq.axonserver.grpc.internal.User.newBuilder()
                                                                                                        .setName(name)
                                                                                                        .build()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Deleting user interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Deleting user failed", e.getCause());
        }
    }
}
