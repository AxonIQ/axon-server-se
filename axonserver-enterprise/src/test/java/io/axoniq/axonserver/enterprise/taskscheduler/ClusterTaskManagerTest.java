package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.enterprise.replication.RaftLeaderProvider;
import io.axoniq.axonserver.grpc.TaskStatus;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import io.axoniq.axonserver.taskscheduler.ScheduledTaskExecutor;
import io.axoniq.axonserver.taskscheduler.Task;
import io.axoniq.axonserver.taskscheduler.TaskRepository;
import io.axoniq.axonserver.taskscheduler.TransientException;
import io.axoniq.axonserver.test.FakeScheduledExecutorService;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class ClusterTaskManagerTest {

    private static final String CONTEXT = "TEST";
    private ClusterTaskManager testSubject;
    private AtomicBoolean transientError = new AtomicBoolean();
    private AtomicBoolean nonTransientError = new AtomicBoolean();
    private AtomicInteger executionAttempts = new AtomicInteger();
    private Map<String, Task> tasks = new HashMap<>();
    private FakeScheduledExecutorService scheduler = new FakeScheduledExecutorService();

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
            long after = invocationOnMock.getArgument(1);
            long before = invocationOnMock.getArgument(2);
            return tasks.values()
                        .stream()
                        .filter(t -> t.getTimestamp() < before)
                        .filter(t -> t.getTimestamp() >= after)
                        .filter(t -> TaskStatus.SCHEDULED.equals(t.getStatus()))
                        .collect(Collectors.toList());
        }).when(repository).findScheduled(anyString(), anyLong(), anyLong());

        doAnswer(invocation -> {
                     Task task = invocation.getArgument(0);
                     tasks.put(task.getTaskId(), task);
                     return task;
                 }
        ).when(repository).save(any(Task.class));

        doAnswer(invocation -> {
            Task t = invocation.getArgument(0);
            tasks.remove(t.getTaskId());
            return null;
        }).when(repository).delete(any(Task.class));
        when(repository.findById(anyString()))
                .then(invocation -> Optional.ofNullable(tasks.get(invocation.getArgument(0))));

        TaskResultPublisher resultPublisher = (context, taskId, status, newSchedule, retry, message) -> {
            testSubject.updateTask(updateTask(taskId, status, newSchedule, retry, message));
            return CompletableFuture.completedFuture(null);
        };

        testSubject = new ClusterTaskManager(executor,
                                             repository,
                                             resultPublisher,
                                             new RaftLeaderProvider() {
                                                 @Override
                                                 public String getLeader(String context) {
                                                     return null;
                                                 }

                                                 @Override
                                                 public boolean isLeader(String context) {
                                                     return true;
                                                 }

                                          @Override
                                          public Set<String> leaderFor() {
                                              return Collections.emptySet();
                                          }
                                      },
                                             new PlatformTransactionManager() {
                                                 @Override
                                                 public TransactionStatus getTransaction(
                                                         TransactionDefinition definition)
                                                         throws TransactionException {
                                                     return new SimpleTransactionStatus();
                                                 }

                                                 @Override
                                                 public void commit(TransactionStatus status)
                                                         throws TransactionException {

                                                 }

                                                 @Override
                                                 public void rollback(TransactionStatus status)
                                                         throws TransactionException {

                                                 }
                                             },
                                             scheduler,
                                             scheduler.clock());

        testSubject.start();
    }

    @NotNull
    private UpdateTask updateTask(String taskId, TaskStatus status, Long newSchedule, long retry, String message) {
        return UpdateTask.newBuilder()
                         .setTaskId(taskId)
                         .setStatus(status == null ? TaskStatus.FAILED : status)
                         .setInstant(Optional.of(newSchedule).orElse(0L))
                         .setRetryInterval(retry)
                         .setErrorMessage(message == null ? "" : message)
                         .build();
    }

    @Test
    public void checkForTasksInFuture() {
        ScheduleTask task = scheduleTask(1000);
        testSubject.schedule(CONTEXT, task);
        assertEquals(1, tasks.size());
        scheduler.timeElapses(0);
        assertEquals(0, executionAttempts.get());
        scheduler.timeElapses(1000);
        assertEquals(1, executionAttempts.get());
        assertEquals(0, tasks.size());
    }

    @NotNull
    private ScheduleTask scheduleTask(long delay) {
        return ScheduleTask.newBuilder()
                           .setTaskId(UUID.randomUUID().toString())
                           .setInstant(scheduler.clock().millis() + delay)
                           .setRetryInterval(1000)
                           .build();
    }

    @Test
    public void checkForTasksTransientError() {
        transientError.set(true);
        ScheduleTask task = scheduleTask(0);
        testSubject.schedule(CONTEXT, task);
        scheduler.timeElapses(0);
        assertEquals(1, executionAttempts.get());
        assertEquals(TaskStatus.SCHEDULED, tasks.get(task.getTaskId()).getStatus());
        scheduler.timeElapses(1000, TimeUnit.MILLISECONDS);
        assertEquals(2, executionAttempts.get());
        scheduler.timeElapses(1001, TimeUnit.MILLISECONDS);
        assertEquals(2, executionAttempts.get());
        transientError.set(false);
        scheduler.timeElapses(1001, TimeUnit.MILLISECONDS);
        assertEquals(3, executionAttempts.get());
        assertEquals(0, tasks.size());
    }

    @Test
    public void rescheduleTaskEarlier() {
        ScheduleTask task = scheduleTask(1000);
        testSubject.schedule(CONTEXT, task);
        scheduler.timeElapses(500);
        assertEquals(1, tasks.size());
        testSubject.updateTask(updateTask(task.getTaskId(),
                                          TaskStatus.SCHEDULED,
                                          scheduler.clock().millis() + 100,
                                          1000,
                                          null));
        scheduler.timeElapses(100);
        assertEquals(0, tasks.size());
    }

    @Test
    public void rescheduleTaskLater() {
        ScheduleTask task = scheduleTask(1000);
        testSubject.schedule(CONTEXT, task);
        scheduler.timeElapses(500);
        assertEquals(1, tasks.size());
        testSubject.updateTask(updateTask(task.getTaskId(),
                                          TaskStatus.SCHEDULED,
                                          scheduler.clock().millis() + 1000,
                                          1000,
                                          null));
        scheduler.timeElapses(600);
        assertEquals(1, tasks.size());
        scheduler.timeElapses(900);
        assertEquals(0, tasks.size());
    }

    @Test
    public void cancelTask() {
        ScheduleTask task = scheduleTask(1000);
        testSubject.schedule(CONTEXT, task);
        assertEquals(1, tasks.size());
        scheduler.timeElapses(500);
        testSubject.updateTask(updateTask(task.getTaskId(), TaskStatus.CANCELLED, 0L, 1000, null));
        assertEquals(0, tasks.size());
    }

    @Test
    public void checkForTasksNonTransientError() {
        nonTransientError.set(true);
        ScheduleTask task = scheduleTask(0);
        testSubject.schedule(CONTEXT, task);
        scheduler.timeElapses(0);
        assertEquals(1, executionAttempts.get());
        assertEquals(TaskStatus.FAILED, tasks.get(task.getTaskId()).getStatus());
        scheduler.timeElapses(10000, TimeUnit.MILLISECONDS);
        assertEquals(1, executionAttempts.get());
    }
}