package io.axoniq.axonserver.access.role;

import io.axoniq.axonserver.access.jpa.Role;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Created by Sara Pellegrini on 08/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface RoleRepository extends JpaRepository<Role, String> {

    List<Role> findByTypesContains(Role.Type type);

}
