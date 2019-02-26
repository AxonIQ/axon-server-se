package io.axoniq.axonserver.rest;


import io.axoniq.axonserver.access.jpa.User;

import java.util.List;


/**
 * @author Marc Gathier
 */
public interface UserControllerFacade {

    void updateUser(String userName, String password, String[] roles);

    List<User> getUsers();

    void deleteUser(String name);
}
