package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.internal.Action;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationRole;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ProtoConverterTest {
    private Application application;

    @Before
    public void setUp() throws Exception {
        application = Application.newBuilder()
                .setAction(Action.MERGE)
                .setDescription("Description")
                .setHashedToken("1234")
                .setName("Name")
                .addRoles(ApplicationRole.newBuilder().setName("Role1").setEndDate(1000L).build())
                .addRoles(ApplicationRole.newBuilder().setName("Role2").setEndDate(2000L).build())
                .build();
    }

    @Test
    public void createApplication() throws Exception {
        io.axoniq.platform.application.jpa.Application jpaApplication = ProtoConverter.createJpaApplication(application);
        assertEquals("Description", jpaApplication.getDescription());
        assertEquals("1234", jpaApplication.getHashedToken());
        assertEquals("Name", jpaApplication.getName());
        assertEquals(2, jpaApplication.getRoles().size());
        assertEquals((Long)1000L,
                jpaApplication.getRoles().stream().filter(r -> r.getRole().equals("Role1")).map(r -> r.getEndDate().getTime()).findFirst().orElse(0L));
        assertEquals((Long)2000L,
                jpaApplication.getRoles().stream().filter(r -> r.getRole().equals("Role2")).map(r -> r.getEndDate().getTime()).findFirst().orElse(0L));


        Application application2 = ProtoConverter.createApplication(jpaApplication, Action.MERGE);
        assertEquals("Description", application2.getDescription());
        assertEquals("1234", application2.getHashedToken());
        assertEquals("Name", application2.getName());
        assertEquals(2, application2.getRolesCount());
        assertEquals((Long)1000L,
                application2.getRolesList().stream().filter(r -> r.getName().equals("Role1")).map(ApplicationRole::getEndDate).findFirst().orElse(0L));
        assertEquals((Long)2000L,
                application2.getRolesList().stream().filter(r -> r.getName().equals("Role2")).map(ApplicationRole::getEndDate).findFirst().orElse(0L));


    }

}
