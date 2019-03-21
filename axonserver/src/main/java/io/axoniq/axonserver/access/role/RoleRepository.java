package io.axoniq.axonserver.access.role;

import io.axoniq.axonserver.access.jpa.Role;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 *  Spring Data JpaRepostory to access {@link Role} entities.
 *
 * @author Sara Pellegrini
 */
public interface RoleRepository extends JpaRepository<Role, String> {

    /**
     * Finds defined roles based on the type.
     * @param type Application or User roles
     * @return list of roles
     */
    List<Role> findByTypesContains(Role.Type type);

}
