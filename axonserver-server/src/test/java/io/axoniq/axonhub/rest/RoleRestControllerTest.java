package io.axoniq.axonhub.rest;

import io.axoniq.platform.role.Role;
import io.axoniq.platform.role.RoleController;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.Collection;
import java.util.List;

import static io.axoniq.platform.role.Role.Type.APPLICATION;
import static io.axoniq.platform.role.Role.Type.USER;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

/**
 * Created by Sara Pellegrini on 09/03/2018.
 * sara.pellegrini@gmail.com
 */
@RunWith(MockitoJUnitRunner.class)
public class RoleRestControllerTest {

    private RoleRestController testSubject;

    @Mock
    private RoleController roleController;

    @Before
    public void setup(){
        testSubject = new RoleRestController(roleController);
        List<Role> userRoles = asList(new Role("ADMIN", USER, APPLICATION), new Role("USER", USER));
        when(roleController.listUserRoles()).thenReturn(userRoles);
        List<Role> applicationRoles = asList(new Role("ADMIN", USER, APPLICATION), new Role("READ", APPLICATION));
        when(roleController.listApplicationRoles()).thenReturn(applicationRoles);
    }

    @Test
    public void listUserRolesTest(){
        Collection<String> roles = testSubject.listUserRoles();
        assertThat(roles, contains("ADMIN", "USER"));
        assertThat(roles.size(), equalTo(2));
    }

    @Test
    public void listApplicationRolesTest(){
        Collection<String> roles = testSubject.listApplicationRoles();
        assertThat(roles, contains("ADMIN", "READ"));
        assertThat(roles.size(), equalTo(2));
    }




}
