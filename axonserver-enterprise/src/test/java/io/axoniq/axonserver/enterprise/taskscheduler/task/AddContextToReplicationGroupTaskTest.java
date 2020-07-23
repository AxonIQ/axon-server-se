package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigService;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AddContextToReplicationGroupTaskTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RaftConfigServiceFactory raftServiceFactory;

    @Mock
    private RaftConfigService raftConfigService;

    private AddContextToReplicationGroupTask testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new AddContextToReplicationGroupTask(raftServiceFactory);
    }

    @Test
    public void whenTaskIsExecutedThenItTriggersAddContextMethod() {
        when(raftConfigService.addContext(any(),any(),any())).thenReturn(CompletableFuture.runAsync(()->{}));
        when(raftServiceFactory.getRaftConfigService()).thenReturn(raftConfigService);

        AddContextToReplicationGroup addContextToReplicationGroup = new AddContextToReplicationGroup("rp","ctx", Collections.emptyMap());
        testSubject.executeAsync("", addContextToReplicationGroup);

        verify(raftConfigService).addContext("rp","ctx", Collections.emptyMap());
    }

}