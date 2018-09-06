package io.axoniq.axonhub.rest;

import io.axoniq.platform.role.Role;
import io.axoniq.platform.role.RoleController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

import static java.util.stream.Collectors.toList;

/**
 * Created by Sara Pellegrini on 06/03/2018.
 * sara.pellegrini@gmail.com
 */
@RestController
@RequestMapping("v1/roles")
public class RoleRestController {

    private final RoleController roleController;

    public RoleRestController(RoleController roleController) {
        this.roleController = roleController;
    }

    @GetMapping("user")
    Collection<String> listUserRoles(){
        return roleController
                .listUserRoles()
                .stream().map(Role::name).sorted().collect(toList());
    }

    @GetMapping("application")
    Collection<String> listApplicationRoles(){
        return roleController
                .listApplicationRoles()
                .stream().map(Role::name).sorted().collect(toList());
    }



}
