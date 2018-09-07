package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.Application;
import io.axoniq.platform.application.jpa.ApplicationRole;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@SpringBootTest(classes = ApplicationRepository.class)
@EnableAutoConfiguration
@EntityScan("io.axoniq.platform")
public class ApplicationControllerTest {
    private ApplicationController testSubject;

    @Autowired
    private ApplicationRepository applicationRepository;
    @Autowired
    private ApplicationModelVersionRepository versionRepository;
    private BcryptHasher hasher =  new BcryptHasher();


    @Before
    public void setUp() {
        testSubject = new ApplicationController(applicationRepository, versionRepository, hasher);
    }

    @Test
    public void getApplications() {
        assertTrue(1 < testSubject.getApplications().size());
    }

    @Test
    public void updateNew() {
        AtomicBoolean updateListenerCalled = new AtomicBoolean(false);
        testSubject.registerUpdateListener("sample", app -> updateListenerCalled.set(true));
        long modelVersion = testSubject.getModelVersion();
        assertNotEquals( "Token already returned", testSubject.updateJson(new Application("Test1", "TEST1", null,
                null, new ApplicationRole("READ", "default", null))).getTokenString());
        assertEquals(modelVersion+1, testSubject.getModelVersion());
        assertTrue(updateListenerCalled.get());
    }

    @Test
    public void updateExisting() {
        assertEquals("Token already returned", testSubject.updateJson(new Application("Test", "TEST1", null,
                null, new ApplicationRole("READ", "default", null))).getTokenString());
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

    @Test
    public void updateModelVersion() {
        testSubject.updateModelVersion(100);
        assertEquals(100, testSubject.getModelVersion());
    }

}