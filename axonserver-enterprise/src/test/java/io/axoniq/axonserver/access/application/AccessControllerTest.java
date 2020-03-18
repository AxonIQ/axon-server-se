package io.axoniq.axonserver.access.application;

import com.google.common.collect.Sets;
import io.axoniq.axonserver.access.jpa.FunctionRole;
import io.axoniq.axonserver.access.jpa.PathToFunction;
import io.axoniq.axonserver.access.jpa.Role;
import io.axoniq.axonserver.access.roles.FunctionRoleRepository;
import io.axoniq.axonserver.access.roles.PathToFunctionRepository;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;
import org.mockito.stubbing.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
    private JpaContextApplicationRepository applicationRepository;

    @Mock
    private JpaApplicationRepository centralApplicationRepository;


    @Before
    public void setup() {
        Hasher hasher = new BcryptHasher();
        List<JpaContextApplication> applications = new ArrayList<>();
        JpaContextApplication app = new JpaContextApplication("default", "Test");
        app.setHashedToken(hasher.hash("1234567890"));
        app.setTokenPrefix("12345678");
        app.setRoles(Collections.singleton("READ"));
        applications.add(app);
        app = new JpaContextApplication("context2", "Test");
        app.setHashedToken(hasher.hash("1234567890"));
        app.setTokenPrefix("12345678");
        app.setRoles(Collections.singleton("WRITE"));
        applications.add(app);

        when(applicationRepository.findAllByTokenPrefixAndContext(any(), any()))
                .thenAnswer((Answer<List<JpaContextApplication>>) invocationOnMock -> {
                    String prefix = invocationOnMock.getArgument(0);
                    String context = invocationOnMock.getArgument(1);
                    return applications.stream()
                                       .filter(app1 -> app1.getTokenPrefix().equals(prefix))
                                       .filter(app1 -> app1.getContext().equals(context))
                                       .collect(Collectors.toList());
                });
        when(applicationRepository.findAllByTokenPrefix(any()))
                .thenAnswer((Answer<List<JpaContextApplication>>) invocationOnMock -> {
                    String prefix = invocationOnMock.getArgument(0);
                    return applications.stream()
                                       .filter(app1 -> app1.getTokenPrefix().equals(prefix))
                                       .collect(Collectors.toList());
                });

        PathToFunctionRepository pathToFunctionRepository = mock(PathToFunctionRepository.class);
        FunctionRoleRepository funtionRoleRepository = mock(FunctionRoleRepository.class);

        List<PathToFunction> pathMappings = new ArrayList<>();
        pathMappings.add(pathToFunction("path3/[^/]*/demo", "READ"));
        pathMappings.add(pathToFunction("path1", "READ"));
        pathMappings.add(pathToFunction("path2", "WRITE"));
        pathMappings.add(pathToFunction("adminPath", "ADMIN"));

        when(pathToFunctionRepository.findAll()).thenReturn(pathMappings);

        when(pathToFunctionRepository.findById(any()))
                .thenAnswer((Answer<Optional<PathToFunction>>) invocationOnMock -> {
                    String path = invocationOnMock.getArgument(0);
                    for (PathToFunction pathMapping : pathMappings) {
                        if (pathMapping.getPath().equals(path)) {
                            return Optional.of(pathMapping);
                        }
                    }
                    return Optional.empty();
                });

        when(funtionRoleRepository.findByFunction(any()))
                .thenAnswer((Answer<Collection<FunctionRole>>) invocationOnMock -> {
                    String path = invocationOnMock.getArgument(0);
                    switch (path) {
                        case "READ":
                            return Collections.singleton(functionRole("READ"));
                        case "WRITE":
                            return Collections.singleton(functionRole("WRITE"));
                        case "ADMIN":
                            return Collections.singleton(functionRole("ADMIN"));
                    }
                    return Collections.emptySet();
                });

        testSubject = new AccessControllerDB(applicationRepository,
                                             pathToFunctionRepository,
                                             funtionRoleRepository,
                                             hasher, () -> "This is the system token");
    }

    private FunctionRole functionRole(String write) {
        FunctionRole functionRole = new FunctionRole();
        Role role = new Role();
        role.setRole(write);
        functionRole.setRole(role);
        functionRole.setFunction(write);
        return functionRole;
    }

    private PathToFunction pathToFunction(String path, String read) {
        PathToFunction pathToFunction = new PathToFunction();
        pathToFunction.setFunction(read);
        pathToFunction.setPath(path);
        return pathToFunction;
    }

    @Test
    public void authorizeNonExistingToken() {
        assertFalse(testSubject.authorize("12349999", "default", "path1"));
    }

    @Test
    public void authorizeMissingPath() {
        assertTrue(testSubject.authorize("1234567890", "default", "path4"));
    }

    @Test
    public void authorizeMissingRole() {
        assertFalse(testSubject.authorize("1234567890", "default", "path2"));
    }

    @Test
    public void authorizeWithRole() {
        assertTrue(testSubject.authorize("1234567890", "default", "path1"));
        assertTrue(testSubject.authorize("1234567890", "context2", "path2"));
    }

    @Test
    public void authorizeWithSystemToken() {
        assertTrue(testSubject.authorize("This is the system token", "_admin", "adminPath"));
        assertFalse(testSubject.authorize("This is the system token", "default", "adminPath"));
        assertFalse(testSubject.authorize("This is the system token", "default", "path1"));
    }

    @Test
    public void authorizeWithWildcard() {
        assertTrue(testSubject.authorize("1234567890", "default", "path3/test/demo"));
    }

    @Test
    public void getRoles() {
        assertEquals(Sets.newHashSet("READ@default", "WRITE@context2"), testSubject.getRoles("1234567890"));
    }
}