package io.axoniq.platform.role;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Created by Sara Pellegrini on 08/03/2018.
 * sara.pellegrini@gmail.com
 */
public interface RoleRepository extends JpaRepository<Role, String> {

    List<Role> findByTypesContains(Role.Type type);

}
