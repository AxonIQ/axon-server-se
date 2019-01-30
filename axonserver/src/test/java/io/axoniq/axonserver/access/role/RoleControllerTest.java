package io.axoniq.axonserver.access.role;

import io.axoniq.axonserver.AxonServer;
import io.axoniq.axonserver.access.jpa.Role;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Created by Sara Pellegrini on 08/03/2018.
 * sara.pellegrini@gmail.com
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@ComponentScan(basePackages = "io.axoniq.axonserver.access", lazyInit = true)
@ContextConfiguration(classes = AxonServer.class)
public class RoleControllerTest {

    private RoleController roleController;

    @Autowired
    private RoleRepository roleRepository;

    @Before
    public void before(){
        roleController = new RoleController(roleRepository);
    }

    @Test
    public void testFindUserRoles(){
        List<Role> roles = roleController.listUserRoles();
        assertThat(roles.size(), equalTo(2));
        List<String> roleNames = roles.stream().map(Role::name).sorted().collect(toList());
        assertThat(roleNames, contains("ADMIN","READ" ));
    }


    @Test
    public void testFindApplicationRoles(){
        List<Role> roles = roleController.listApplicationRoles();
        assertThat(roles.size(), equalTo(3));
        List<String> roleNames = roles.stream().map(Role::name).sorted().collect(toList());
        assertThat(roleNames, contains("ADMIN", "READ","WRITE" ));
    }



}
