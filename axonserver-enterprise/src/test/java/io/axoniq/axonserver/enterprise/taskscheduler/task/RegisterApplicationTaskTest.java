package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigService;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class RegisterApplicationTaskTest {

    @Mock
    private StandaloneTaskManager taskManager;

    @Mock
    private RaftConfigServiceFactory raftServiceFactory;

    @Mock
    private RaftConfigService raftConfigService;

    private RegisterApplicationTask testSubject;

    RegisterApplicationTask.RegisterApplicationPayload registerApplicationPayload
            = new RegisterApplicationTask.RegisterApplicationPayload(
            new ClusterTemplate.Application("app",
                    "desc",Collections.emptyList(),
                    "token"));

    @Before
    public void setUp() {
        testSubject = new RegisterApplicationTask(taskManager, raftServiceFactory);
        when(raftServiceFactory.getRaftConfigService()).thenReturn(raftConfigService);
    }

    @Test
    public void whenTaskIsExecutedThenUserControllerShouldBeCalled() {
        doReturn(null).when(raftConfigService).updateApplication(any());

        testSubject.execute("",registerApplicationPayload);

        ArgumentCaptor<Application> argumentCaptor = ArgumentCaptor.forClass(Application.class);
        verify(raftConfigService).updateApplication(argumentCaptor.capture());
        Application app = argumentCaptor.getValue();

        assertEquals("app", app.getName());
        assertEquals("desc", app.getDescription());
        assertEquals("token", app.getToken());
    }

    @Test(expected = TransientException.class)
    public void whenContextNotFoundThenThrowTransientException() {
        doThrow(new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,"")).when(raftConfigService).updateApplication(any());

        testSubject.execute("",registerApplicationPayload);

        ArgumentCaptor<Application> argumentCaptor = ArgumentCaptor.forClass(Application.class);
        verify(raftConfigService).updateApplication(argumentCaptor.capture());
        Application app = argumentCaptor.getValue();

        assertEquals("app", app.getName());
        assertEquals("desc", app.getDescription());
        assertEquals("token", app.getToken());
    }

    @Test(expected = IllegalAccessError.class)
    public void whenUnknownExceptionThenThrowException() {
        doThrow(new IllegalAccessError("")).when(raftConfigService).updateApplication(any());
        testSubject.execute("",registerApplicationPayload);
    }


    @Test
    public void whenTaskIsScheduledThenTaskManagerCreatesTask(){
        doNothing().when(taskManager).createTask(any(),any(),any());

        testSubject.schedule(registerApplicationPayload.getApplication());

        ArgumentCaptor<RegisterApplicationTask.RegisterApplicationPayload> argumentCaptor = ArgumentCaptor.forClass(RegisterApplicationTask.RegisterApplicationPayload.class);

        verify(taskManager).createTask(eq(RegisterApplicationTask.class.getName()),
                argumentCaptor.capture(),
                eq(Duration.ZERO));

        RegisterApplicationTask.RegisterApplicationPayload app = argumentCaptor.getValue();
        assertEquals("app", app.getApplication().getName());
        assertEquals("desc", app.getApplication().getDescription());
        assertEquals("token", app.getApplication().getToken());

    }

}