package io.axoniq.axonserver.rest;


import io.axoniq.axonserver.access.jpa.User;

import java.util.List;


/**
 * Author: marc
 */
public interface UserControllerFacade {

    User updateUser(String userName, String password, String[] roles);

    List<User> getUsers();

    void deleteUser(String name);
}
