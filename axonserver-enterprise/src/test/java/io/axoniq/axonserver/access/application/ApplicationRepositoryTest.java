package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.AxonServer;
import io.axoniq.axonserver.access.jpa.Application;
import io.axoniq.axonserver.access.jpa.ApplicationContext;
import io.axoniq.axonserver.access.jpa.ApplicationContextRole;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@ComponentScan(basePackages = "io.axoniq.axonserver.access", lazyInit = true)
@ContextConfiguration(classes = AxonServer.class)
public class ApplicationRepositoryTest {

    @Autowired
    private ApplicationRepository applicationRepository;

    @Test
    public void testHasRole() {
        assertNotNull(applicationRepository);

        applicationRepository.save(new Application("TestApp", "Description", "1235-567", "1235-5678",
                                                   new ApplicationContext("test", Arrays.asList(
                                                           new ApplicationContextRole("READ"), new ApplicationContextRole("WRITE")
                                                   ))));
        applicationRepository.save(new Application("TestApp2", "Description", "1235-567","1235-5679",
                                                   new ApplicationContext("test", Collections.singletonList(
                                                           new ApplicationContextRole("WRITE")
                                                   ))));
        Application app1 = applicationRepository.findFirstByName("TestApp");
        assertNotNull(app1);
        assertTrue(app1.hasRoleForContext("READ", "test"));
        assertTrue(app1.hasRoleForContext("WRITE", "test"));
        app1 = applicationRepository.findFirstByName("TestApp2");
        assertFalse(app1.hasRoleForContext("READ", "test"));
        assertTrue(app1.hasRoleForContext("WRITE", "test"));
    }

    @Test
    public void testFindWithoutPrefix() {
        applicationRepository.save(new Application("TestAppWIthoutPrefix", "Description", null, "9999-5678",
                                                   new ApplicationContext("test", Arrays.asList(
                                                           new ApplicationContextRole("READ"), new ApplicationContextRole("WRITE")
                                                   ))));
        Application withoutPrefix = applicationRepository.findAllByTokenPrefix(null).stream().filter(a -> a.getName().equals("TestAppWIthoutPrefix")).findFirst().orElse(null);
        assertNotNull(withoutPrefix);

    }

    @Test(expected = DataIntegrityViolationException.class)
    public void testDuplicateName() {
        applicationRepository.save(new Application("TestApp3", "Description",  "1234-567", "1234-5678"));
        applicationRepository.save(new Application("TestApp3", "Description", "1234-567", "1234-5679"));
    }

    @Test(expected = DataIntegrityViolationException.class)
    public void testDuplicateToken() {
        applicationRepository.save(new Application("TestApp4", "Description", "1236-567", "1236-5678"));
        applicationRepository.save(new Application("TestApp5", "Description", "1236-567", "1236-5678"));
    }


}
