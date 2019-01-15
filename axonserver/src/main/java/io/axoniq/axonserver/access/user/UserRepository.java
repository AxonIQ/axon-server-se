package io.axoniq.axonserver.access.user;

import io.axoniq.axonserver.access.jpa.User;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface UserRepository extends JpaRepository<User, String> {

}
