package io.axoniq.platform.user;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Author: marc
 */
public interface UserRepository extends JpaRepository<User, String> {

}
