package io.axoniq.platform.user;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * @author Marc Gathier
 */
public interface UserRepository extends JpaRepository<User, String> {

}
