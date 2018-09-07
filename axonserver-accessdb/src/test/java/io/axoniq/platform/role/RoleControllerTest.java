package io.axoniq.platform.role;

import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
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
@SpringBootTest(classes = RoleRepository.class)
@EnableAutoConfiguration
@EntityScan("io.axoniq.platform")
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
        assertThat(roles.size(), equalTo(1));
        assertThat(roles.get(0).name(), equalTo("TEST_USER_ROLE"));
    }


    @Test
    public void testFindApplicationRoles(){
        List<Role> roles = roleController.listApplicationRoles();
        assertThat(roles.size(), equalTo(2));
        List<String> roleNames = roles.stream().map(Role::name).collect(toList());
        assertThat(roleNames, contains("TEST_APPLICATION_ROLE","TEST_APPLICATION_ROLE_2" ));
    }



}
