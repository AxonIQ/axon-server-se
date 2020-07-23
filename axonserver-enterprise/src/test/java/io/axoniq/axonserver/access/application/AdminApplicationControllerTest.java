package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.AxonServer;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.persistence.EntityManager;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@Transactional
@ComponentScan(basePackages = "io.axoniq.axonserver.access", lazyInit = true)
@ContextConfiguration(classes = AxonServer.class)
public class AdminApplicationControllerTest {

    private AdminApplicationController testSubject;

    @Autowired
    private AdminApplicationRepository applicationRepository;
    @Autowired
    private EntityManager entityManager;
    private BcryptHasher hasher = new BcryptHasher();

    @MockBean
    private VersionInfoProvider versionInfoProvider;


    @Before
    public void setUp() {
        testSubject = new AdminApplicationController(applicationRepository, hasher);
        //-- insert into application(id, description, name, hashed_token) values( 1000, 'TEST', 'Test', 'AAAA')
        //-- insert into application(id, description, name, hashed_token) values( 2000, 'TestApplication for Delete', 'Delete', 'BBBB')
        entityManager.persist(new AdminApplication("Test", "TEST", null, "AAAA"));
        entityManager.persist(new AdminApplication("Delete", "TestApplication for Delete", null, "BBBB"));
    }

    @Test
    public void getApplications() {
        assertTrue(1 < testSubject.getApplications().size());
    }

    @Test
    public void delete() {
        AtomicBoolean deleteListenerCalled = new AtomicBoolean(false);
        int count = testSubject.getApplications().size();
        testSubject.delete("Delete");
        assertEquals(count-1, testSubject.getApplications().size());
    }

    @Test
    public void deleteNonExisting() {
        testSubject.delete("DeleteAnother");
    }


    @Test
    public void getApplication() {
        AdminApplication application = testSubject.get("Test");
        assertNotNull(application);
        assertEquals("Test", application.getName());
    }

    @Test(expected = ApplicationNotFoundException.class)
    public void getUnknownApplication() {
        testSubject.get("TEST1");
    }

    @Test
    public void syncApplication() {
        testSubject.synchronize(new AdminApplication("SYNC", "Synched application", null, null));
        AdminApplication app = testSubject.get("SYNC");
        assertEquals("Synched application", app.getDescription());
    }


}
