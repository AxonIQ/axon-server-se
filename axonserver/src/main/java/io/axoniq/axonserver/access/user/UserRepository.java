package io.axoniq.axonserver.access.user;

import io.axoniq.axonserver.access.jpa.User;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface UserRepository extends JpaRepository<User, String> {

}
