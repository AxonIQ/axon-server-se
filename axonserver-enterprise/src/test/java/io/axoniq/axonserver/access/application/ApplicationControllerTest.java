package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.AxonServer;
import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.ApplicationContext;
import io.axoniq.axonserver.access.jpa.ApplicationContextRole;
import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
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
public class ApplicationControllerTest {
    private ApplicationController testSubject;

    @Autowired
    private ApplicationRepository applicationRepository;
    @Autowired
    private EntityManager entityManager;
    private BcryptHasher hasher =  new BcryptHasher();


    @Before
    public void setUp() {
        testSubject = new ApplicationController(applicationRepository, hasher);
        //-- insert into application(id, description, name, hashed_token) values( 1000, 'TEST', 'Test', 'AAAA')
        //-- insert into application(id, description, name, hashed_token) values( 2000, 'TestApplication for Delete', 'Delete', 'BBBB')
        entityManager.persist(new Application("Test", "TEST", null, "AAAA"));
        entityManager.persist(new Application("Delete", "TestApplication for Delete", null, "BBBB"));
    }

    @Test
    public void getApplications() {
        assertTrue(1 < testSubject.getApplications().size());
    }

    @Test
    public void updateNew() {
        AtomicBoolean updateListenerCalled = new AtomicBoolean(false);
        testSubject.registerUpdateListener("sample", app -> updateListenerCalled.set(true));
        assertNotEquals( "Token already returned", testSubject.updateJson(new Application("Test1", "TEST1", null,
                                                                                          null, new ApplicationContext("default", Collections.singletonList(new ApplicationContextRole("READ"))))).getTokenString());
        assertTrue(updateListenerCalled.get());
    }

    @Test
    public void updateExisting() {
        assertEquals("Token already returned", testSubject.updateJson(new Application("Test", "TEST1", null,
                null, new ApplicationContext("default", Collections.singletonList(new ApplicationContextRole("READ")))))
                                                                              .getTokenString());
    }


    @Test
    public void delete() {
        AtomicBoolean deleteListenerCalled = new AtomicBoolean(false);
        testSubject.registerDeleteListener("sample", app -> deleteListenerCalled.set(true));
        int count = testSubject.getApplications().size();
        testSubject.delete("Delete");
        assertEquals(count-1, testSubject.getApplications().size());
        assertTrue(deleteListenerCalled.get());
    }
    @Test(expected = ApplicationNotFoundException.class)
    public void deleteNonExisting() {
        testSubject.delete("DeleteAnother");
    }


    @Test
    public void getApplication() {
        Application application = testSubject.get("Test");
        assertNotNull(application);
        assertEquals("Test", application.getName());
    }

    @Test(expected = ApplicationNotFoundException.class)
    public void getUnknownApplication() {
        testSubject.get("TEST1");
    }

    @Test
    public void syncApplication() {
        testSubject.synchronize(new Application("SYNC", "Synched application", null, null));
    }


}
