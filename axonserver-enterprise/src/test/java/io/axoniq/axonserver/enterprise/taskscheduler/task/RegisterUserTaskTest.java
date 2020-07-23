package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.access.user.UserControllerFacade;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RegisterUserTaskTest {

    @Mock
    private StandaloneTaskManager taskManager;

    @Mock
    private UserControllerFacade userController;

    private RegisterUserTask testSubject;

    @Before
    public void setUp() {
        testSubject = new RegisterUserTask(taskManager, userController);
    }

    @Test
    public void whenTaskIsExecutedThenUserControllerShouldBeCalled() {
        doNothing().when(userController).updateUser(any(),any(),any());
        RegisterUserTask.RegisterUserPayload registerUserPayload =
                new RegisterUserTask.RegisterUserPayload("user","pass", Collections.emptySet());
        testSubject.execute("",registerUserPayload);

        verify(userController).updateUser("user","pass", Collections.emptySet());
    }

    @Test(expected = TransientException.class)
    public void whenContextNotFoundThenThrowTransientException(){
        doThrow(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,"")).when(userController).updateUser(any(),any(),any());

        RegisterUserTask.RegisterUserPayload registerUserPayload =
                new RegisterUserTask.RegisterUserPayload("user","pass", Collections.emptySet());
        testSubject.execute("",registerUserPayload);

        verify(userController, times(1)).updateUser("user","pass", Collections.emptySet());
    }

    @Test(expected = IllegalAccessError.class)
    public void whenUnknownExceptionThenThrowException(){
        doThrow(new IllegalAccessError("")).when(userController).updateUser(any(),any(),any());

        RegisterUserTask.RegisterUserPayload registerUserPayload =
                new RegisterUserTask.RegisterUserPayload("user","pass", Collections.emptySet());
        testSubject.execute("",registerUserPayload);

        verify(userController, times(1)).updateUser("user","pass", Collections.emptySet());
    }

    @Test
    public void whenTaskIsScheduledThenTaskManagerCreatesTask(){
        doNothing().when(taskManager).createTask(any(),any(),any());

        testSubject.schedule("user","pass", Collections.emptySet());

        verify(taskManager).createTask(eq(RegisterUserTask.class.getName()),
             isA(RegisterUserTask.RegisterUserPayload.class),
                eq(Duration.ZERO));
    }


}