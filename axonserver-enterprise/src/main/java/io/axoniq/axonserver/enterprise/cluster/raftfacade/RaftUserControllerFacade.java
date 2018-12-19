package io.axoniq.axonserver.enterprise.cluster.raftfacade;

import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.rest.UserControllerFacade;
import io.axoniq.platform.user.User;
import io.axoniq.platform.user.UserController;
import io.axoniq.platform.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Author: marc
 */
public class RaftUserControllerFacade implements UserControllerFacade {

    private final UserController userController;
    private final RaftConfigServiceFactory raftServiceFactory;

    public RaftUserControllerFacade(UserController userController, RaftConfigServiceFactory raftServiceFactory) {
        this.userController = userController;
        this.raftServiceFactory = raftServiceFactory;
    }

    @Override
    public void updateUser(String userName, String password, String[] roles) {
        try {
            raftServiceFactory.getRaftConfigService().updateUser(io.axoniq.axonserver.grpc.internal.User.newBuilder()
                                                                                                        .setName(userName)
                                                                                                        .setPassword(
                                                                                                                StringUtils.getOrDefault(password,""))
                                                                                                        .addAllRoles(Arrays.asList(roles))
                                                                                                        .build()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
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
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
