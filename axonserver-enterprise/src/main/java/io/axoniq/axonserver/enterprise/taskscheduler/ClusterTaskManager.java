package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.replication.RaftLeaderProvider;
import io.axoniq.axonserver.grpc.TaskStatus;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import io.axoniq.axonserver.taskscheduler.BaseTaskManager;
import io.axoniq.axonserver.taskscheduler.ScheduledTaskExecutor;
import io.axoniq.axonserver.taskscheduler.Task;
import io.axoniq.axonserver.taskscheduler.TaskPayload;
import io.axoniq.axonserver.taskscheduler.TaskRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * Component that reads tasks from the controldb and tries to execute them.
 * It will only process tasks for contexts where the current node is the leader.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class ClusterTaskManager extends BaseTaskManager {

    private final TaskResultPublisher taskResultPublisher;

    /**
     * Constructor for the task manager.
     *
     * @param taskExecutor               component that will execute the task
     * @param taskRepository             repository of scheduled tasks
     * @param taskResultPublisher        component to publish the result of the task
     * @param raftLeaderProvider         provides the leader of a context
     * @param platformTransactionManager transaction manager
     * @param scheduler                  scheduler component to schedule tasks
     * @param clock                      a clock instance
     */
    public ClusterTaskManager(ScheduledTaskExecutor taskExecutor,
                              TaskRepository taskRepository,
                              TaskResultPublisher taskResultPublisher,
                              RaftLeaderProvider raftLeaderProvider,
                              PlatformTransactionManager platformTransactionManager,
                              @Qualifier("taskScheduler") ScheduledExecutorService scheduler, Clock clock) {
        super(taskExecutor,
              taskRepository,
              raftLeaderProvider::leaderFor,
              raftLeaderProvider::isLeader,
              platformTransactionManager,
              scheduler, clock);
        this.taskResultPublisher = taskResultPublisher;
    }

    /**
     * Stores a new task in the repository to be executed by the scheduler
     *
     * @param context      the context for the task
     * @param scheduleTask the task to schedule
     */
    public void schedule(String context, ScheduleTask scheduleTask) {
        Task task = new Task();
        task.setContext(context);
        task.setPayload(new TaskPayload(scheduleTask.getPayload()));
        task.setRetryInterval(scheduleTask.getRetryInterval());
        task.setTaskExecutor(scheduleTask.getTaskExecutor());
        task.setTaskId(scheduleTask.getTaskId());
        task.setTimestamp(scheduleTask.getInstant());
        task.setStatus(TaskStatus.SCHEDULED);
        saveAndSchedule(task);
    }

    /**
     * Deletes all scheduled tasks for a replicationGroup.
     *
     * @param replicationGroup the name of the replicationGroup
     */
    @Transactional
    public void deleteAllByReplicationGroup(String replicationGroup) {
        unscheduleTasksForReplicationGroup(replicationGroup);
        taskRepository.deleteAllByContext(replicationGroup);
    }

    private void unscheduleTasksForReplicationGroup(String context) {
        Map<String, ScheduledFuture<?>> scheduled = scheduledProcessors.remove(context);
        if (scheduled != null) {
            scheduled.values().forEach(scheduledRegistration -> scheduledRegistration.cancel(false));
        }
    }

    /**
     * Returns all scheduled tasks for a replicationGroup
     *
     * @param replicationGroup the replication group name
     * @return the scheduled tasks
     */
    public List<Task> findAllByReplicationGroup(String replicationGroup) {
        return taskRepository.findAllByContext(replicationGroup);
    }

    /**
     * Event handler for the contextDeleted event. Deletes all tasks for the context.
     *
     * @param event the event
     */
    @EventListener
    @Transactional
    public void on(ClusterEvents.ReplicationGroupDeleted event) {
        logger.debug("Deleting tasks for {}", event.replicationGroup());
        deleteAllByReplicationGroup(event.replicationGroup());
    }


    /**
     * Event handler for the {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.BecomeLeader} event.
     * When
     * node becomes leader for the context, it schedules events for this context.
     *
     * @param becomeLeader the event
     */
    @EventListener
    public void on(ClusterEvents.BecomeLeader becomeLeader) {
        taskRepository.findScheduled(becomeLeader.replicationGroup(), 0, nextTimestamp.get())
                      .forEach(this::doScheduleTask);
    }

    /**
     * Event handler for the {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.LeaderStepDown} event.
     * When the leader steps down for a context, the task manager removes all tasks for that context from the scheduler.
     *
     * @param leaderStepDown the event
     */
    @EventListener
    public void on(ClusterEvents.LeaderStepDown leaderStepDown) {
        unscheduleTasksForReplicationGroup(leaderStepDown.replicationGroup());
    }

    /**
     * Updates a task in the repository. If new status is COMPLETED or CANCELLED, it will delete the task from the
     * database
     *
     * @param updateTask updated task information
     */
    public void updateTask(UpdateTask updateTask) {
        taskRepository.findById(updateTask.getTaskId()).ifPresent(t -> update(t, updateTask));
    }

    @Override
    protected CompletableFuture<Void> processResult(String context, String taskId, TaskStatus status, long newSchedule,
                                                    long retry, String message) {
        return taskResultPublisher.publishResult(context, taskId, status, newSchedule, retry, message);
    }

    private void update(Task task, UpdateTask updateTask) {
        if (raftLeaderTest.test(task.getContext())) {
            unschedule(task.getContext(), task.getTaskId());
        }

        if (TaskStatus.COMPLETED.equals(updateTask.getStatus()) || TaskStatus.CANCELLED.equals(updateTask
                                                                                                       .getStatus())) {
            new TransactionTemplate(platformTransactionManager).execute(status -> {
                taskRepository.delete(task);
                return null;
            });
            return;
        }

        task.setTimestamp(updateTask.getInstant());
        task.setRetryInterval(updateTask.getRetryInterval());
        task.setStatus(updateTask.getStatus());
        task.setMessage(updateTask.getErrorMessage());
        new TransactionTemplate(platformTransactionManager).execute(status -> taskRepository.save(task));

        doScheduleTask(task);
    }


}
