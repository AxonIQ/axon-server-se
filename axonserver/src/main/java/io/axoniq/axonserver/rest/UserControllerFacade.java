package io.axoniq.axonserver.rest;

import io.axoniq.platform.user.User;

import java.util.List;

/**
 * Author: marc
 */
public interface UserControllerFacade {

    void updateUser(String userName, String password, String[] roles);

    List<User> getUsers();

    void deleteUser(String name);
}
