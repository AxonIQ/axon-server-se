package io.axoniq.axonserver.config;

import io.axoniq.axonserver.access.jpa.User;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.applicationevents.UserEvents;
import io.axoniq.axonserver.rest.UserControllerFacade;
import org.junit.*;
import org.mockito.*;
import org.springframework.context.ApplicationEventPublisher;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class AxonServerStandardConfigurationTest {
    private AxonServerStandardConfiguration testSubject = new AxonServerStandardConfiguration();

    @Test
    public void deleteUserRaisesEvent() {

        ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
        UserController userController = mock(UserController.class);
        UserControllerFacade facade = testSubject.userControllerFacade(userController,
                                                                       applicationEventPublisher);
        facade.deleteUser("User");
        verify(applicationEventPublisher).publishEvent(argThat(new ArgumentMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                return o instanceof UserEvents.UserDeleted && ((UserEvents.UserDeleted) o).getName().equals("User");
            }
        }));
    }
    @Test
    public void updateUserRaisesEvent() {

        ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
        UserController userController = mock(UserController.class);
        doAnswer((invocationOnMock ->
                new User((String)invocationOnMock.getArguments()[0], (String)invocationOnMock.getArguments()[1])
                 )).when(userController).updateUser(any(), any(), any());
        UserControllerFacade facade = testSubject.userControllerFacade(userController,
                                                                       applicationEventPublisher);
        facade.updateUser("User", "Password", new String[0]);
        verify(applicationEventPublisher).publishEvent(argThat(new ArgumentMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                return o instanceof UserEvents.UserUpdated&& ((UserEvents.UserUpdated) o).getUser().getUserName().equals("User");
            }
        }));
    }

}