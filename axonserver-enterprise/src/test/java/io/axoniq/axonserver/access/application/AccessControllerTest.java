package io.axoniq.axonserver.access.application;

import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class AccessControllerTest {
    private AccessControllerDB testSubject;

    @Mock
    private PathMappingRepository pathMappingRepository;
    @Mock
    private JpaContextApplicationRepository applicationRepository;


    @Before
    public void setup() {
        Hasher hasher = new BcryptHasher();
        List<PathMapping> mappings = new ArrayList<>();
        PathMapping mapping = new PathMapping("path1", "READ");
        mappings.add(mapping);
        mappings.add(new PathMapping("path2", "WRITE"));
        mappings.add(new PathMapping("path3/*", "READ"));
        when(pathMappingRepository.findAll()).thenReturn(mappings);

        when(pathMappingRepository.findById(any())).thenReturn(Optional.empty());
        when(pathMappingRepository.findById("path1")).thenReturn(Optional.of(mapping));
        List<JpaContextApplication> applications = new ArrayList<>();
        JpaContextApplication app = new JpaContextApplication("default", "Test");
        app.setHashedToken(hasher.hash("1234567890"));
        app.setTokenPrefix("12345678");
        app.setRoles(Collections.singleton("READ"));
        applications.add(app);
        when(applicationRepository.findAllByTokenPrefix(any())).thenReturn(applications);

        testSubject = new AccessControllerDB(applicationRepository, pathMappingRepository, hasher);
    }

    @Test
    public void authorizeNonExistingToken() throws Exception {
        assertFalse(testSubject.authorize("12349999", "default", "path1", true));
    }

    @Test
    public void authorizeMissingPath() throws Exception {
        assertFalse(testSubject.authorize("1234567890", "default","path4", true));
    }

    @Test
    public void authorizeMissingRole() throws Exception {
        assertFalse(testSubject.authorize("1234567890", "default","path2", true));
    }

    @Test
    public void authorizeWithRole() throws Exception {
        assertTrue(testSubject.authorize("1234567890", "default","path1", true));
        assertTrue(testSubject.authorize("1234567890", "default","path1", true));
    }

    @Test
    public void authorizeWithWildcard() throws Exception {
        assertTrue(testSubject.authorize("1234567890", "default","path3/test", true));
    }
}
