package io.axoniq.axonserver.access.role;

/**
 * Created by Sara Pellegrini on 07/03/2018.
 * sara.pellegrini@gmail.com
 */
import io.axoniq.axonserver.access.jpa.Role;
import org.springframework.stereotype.Controller;

import java.util.List;

import static io.axoniq.axonserver.access.jpa.Role.Type.APPLICATION;
import static io.axoniq.axonserver.access.jpa.Role.Type.USER;

@Controller
public class RoleController {

    private final RoleRepository roleRepository;

    public RoleController(RoleRepository roleRepository) {
        this.roleRepository = roleRepository;
    }


    public List<Role> listUserRoles(){
        return roleRepository.findByTypesContains(USER);
    }

    public List<Role> listApplicationRoles(){
        return roleRepository.findByTypesContains(APPLICATION);
    }


}
