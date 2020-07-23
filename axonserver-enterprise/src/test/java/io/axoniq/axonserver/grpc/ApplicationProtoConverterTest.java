package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.access.application.AdminApplicationContext;
import io.axoniq.axonserver.access.application.AdminApplication;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ApplicationProtoConverterTest {
    private Application application;

    @Before
    public void setUp() throws Exception {
        application = Application.newBuilder()
                .setDescription("Description")
                .setName("Name")
                                 .addRolesPerContext(ApplicationContextRole.newBuilder()
                                                                           .setContext("Context")
                                                                           .addRoles("Role1")
                                                                           .addRoles("Role2")
                                                                           .build())
                .build();
    }

    @Test
    public void createApplication() throws Exception {
        AdminApplication jpaApplication = ApplicationProtoConverter
                .createJpaApplication(application);
        assertEquals("Description", jpaApplication.getDescription());
        assertEquals("Name", jpaApplication.getName());
        assertEquals(1, jpaApplication.getContexts().size());
        AdminApplicationContext applicationContext = jpaApplication.getContexts().iterator()
                                                                   .next();

        assertEquals(2, applicationContext.getRoles().size());
        assertTrue(
                applicationContext.getRoles().stream().anyMatch(r -> r.getRole().equals("Role1")));
        assertTrue(
                applicationContext.getRoles().stream().anyMatch(r -> r.getRole().equals("Role2")));


        Application application2 = ApplicationProtoConverter.createApplication(jpaApplication);
        assertEquals("Description", application2.getDescription());
        assertEquals("Name", application2.getName());
        assertEquals(1, application2.getRolesPerContextCount());
        assertEquals(2, application2.getRolesPerContext(0).getRolesCount());
        assertTrue(
                application2.getRolesPerContext(0).getRolesList().stream().anyMatch(r -> r.equals("Role1")));
        assertTrue(
                application2.getRolesPerContext(0).getRolesList().stream()
                            .anyMatch(r -> r.equals("Role2")));


    }

}
