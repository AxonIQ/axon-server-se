package io.axoniq.platform.application;

import io.axoniq.platform.application.jpa.Application;
import io.axoniq.platform.application.jpa.ApplicationRole;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@EnableAutoConfiguration
@SpringBootTest(classes = ApplicationRepository.class)
@EntityScan("io.axoniq.platform")
public class ApplicationRepositoryTest {

    @Autowired
    private ApplicationRepository applicationRepository;

    @Test
    public void testHasRole() {
        assertNotNull(applicationRepository);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, 7);
        Date endDate1 = cal.getTime();
        cal.add(Calendar.DAY_OF_YEAR, 7);
        Date endDate2 = cal.getTime();

        applicationRepository.save(new Application("TestApp", "Description", "1235-567","1235-5678", new ApplicationRole("READ", "test", endDate1), new ApplicationRole("WRITE", "test", endDate1)));
        applicationRepository.save(new Application("TestApp2", "Description", "1235-567","1235-5679", new ApplicationRole("WRITE", "test", endDate2)));
        Application app1 = applicationRepository.findFirstByName("TestApp");
        assertNotNull(app1);
        assertTrue(app1.hasRoleForContext("READ", "test"));
        assertTrue(app1.hasRoleForContext("WRITE", "test"));
        app1 = applicationRepository.findFirstByName("TestApp2");
        assertFalse(app1.hasRoleForContext("READ", "test"));
        assertTrue(app1.hasRoleForContext("WRITE", "test"));
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
