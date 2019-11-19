package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.cluster.internal.FakeClock;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.tasks.Status;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class TaskManagerTest {

    private TaskManager testSubject;
    private AtomicBoolean transientError = new AtomicBoolean();
    private AtomicBoolean nonTransientError = new AtomicBoolean();
    private AtomicBoolean leader = new AtomicBoolean();
    private AtomicInteger executionAttempts = new AtomicInteger();
    private List<Task> tasks = new ArrayList<>();
    private FakeClock clock = new FakeClock();

    @Before
    public void setUp() {
        ScheduledTaskExecutor executor = task -> {
            CompletableFuture<Void> result = new CompletableFuture<>();
            executionAttempts.incrementAndGet();
            if (transientError.get()) {
                result.completeExceptionally(new TransientException("Failed"));
            } else if (nonTransientError.get()) {
                result.completeExceptionally(new RuntimeException("Failed"));
            } else {
                result.complete(null);
            }
            return result;
        };


        TaskRepository repository = mock(TaskRepository.class);
        doAnswer(invocationOnMock -> {
            long timestamp = invocationOnMock.getArgument(0);
            return tasks.stream()
                        .filter(t -> t.getTimestamp() <= timestamp)
                        .filter(t -> Status.SCHEDULED.equals(t.getStatus()))
                        .collect(Collectors.toList());
        }).when(repository).findExecutableTasks(anyLong(), anySet());


        RaftLeaderProvider leaderProvider = new RaftLeaderProvider() {
            @Override
            public String getLeader(String context) {
                return null;
            }

            @Override
            public boolean isLeader(String context) {
                return leader.get();
            }

            @Override
            public String getLeaderOrWait(String context, boolean aBoolean) {
                return null;
            }

            @Override
            public Set<String> leaderFor() {
                return leader.get() ? Collections.singleton(getAdmin()) : Collections.emptySet();
            }
        };
        TaskResultPublisher resultPublisher = (context, taskId, status, newSchedule, retry) -> {
            tasks.stream().filter(t -> taskId.equals(t.getTaskId())).findFirst().ifPresent(t -> {
                t.setStatus(status);
                t.setTimestamp(newSchedule);
                t.setRetryInterval(retry);
            });
            return CompletableFuture.completedFuture(null);
        };
        testSubject = new TaskManager(executor, repository, leaderProvider, resultPublisher, clock);
    }

    @Test
    public void checkForTasksNonLeader() {
        tasks.add(task(0L, Status.SCHEDULED));
        testSubject.checkForTasks();
        assertEquals(0, executionAttempts.get());
    }

    @Test
    public void checkForTasksInFuture() {
        leader.set(true);
        tasks.add(task(clock.millis() + 1000, Status.SCHEDULED));
        testSubject.checkForTasks();
        assertEquals(0, executionAttempts.get());
        clock.add(1000, TimeUnit.MILLISECONDS);
        testSubject.checkForTasks();
        assertEquals(1, executionAttempts.get());
        assertEquals(Status.COMPLETED, tasks.get(0).getStatus());
    }

    @Test
    public void checkForTasksTransientError() {
        leader.set(true);
        transientError.set(true);
        tasks.add(task(0, Status.SCHEDULED));
        testSubject.checkForTasks();
        assertEquals(1, executionAttempts.get());
        assertEquals(Status.SCHEDULED, tasks.get(0).getStatus());
        clock.add(1000, TimeUnit.MILLISECONDS);
        testSubject.checkForTasks();
        assertEquals(2, executionAttempts.get());
        clock.add(1000, TimeUnit.MILLISECONDS);
        testSubject.checkForTasks();
        assertEquals(2, executionAttempts.get());
        clock.add(1000, TimeUnit.MILLISECONDS);
        testSubject.checkForTasks();
        assertEquals(3, executionAttempts.get());
    }

    @Test
    public void checkForTasksNonTransientError() {
        leader.set(true);
        nonTransientError.set(true);
        tasks.add(task(0, Status.SCHEDULED));
        testSubject.checkForTasks();
        assertEquals(1, executionAttempts.get());
        assertEquals(Status.FAILED, tasks.get(0).getStatus());
        clock.add(10000, TimeUnit.MILLISECONDS);
        testSubject.checkForTasks();
        assertEquals(1, executionAttempts.get());
    }

    private Task task(long timestamp, Status status) {
        Task task = new Task();
        task.setContext(getAdmin());
        task.setStatus(status);
        task.setTimestamp(timestamp);
        task.setTaskId(UUID.randomUUID().toString());
        return task;
    }
}