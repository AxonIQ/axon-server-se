package io.axoniq.axonserver.enterprise.taskscheduler;

import io.axoniq.axonserver.enterprise.replication.group.RaftGroupService;
import io.axoniq.axonserver.enterprise.replication.group.RaftGroupServiceFactory;
import io.axoniq.axonserver.grpc.TaskStatus;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the {@link TaskResultPublisher} that distributes the result of a task by appending a raft log entry.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class TaskResultPublisherImpl implements TaskResultPublisher {

    private final RaftGroupServiceFactory raftGroupServiceFactory;

    public TaskResultPublisherImpl(RaftGroupServiceFactory raftGroupServiceFactory) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
    }

    /**
     * Creates a raft entry with type {@link UpdateTask} to update the task in the controldb upon apply.
     *
     * @param context     the context to publish the update in
     * @param taskId      the unique id of the task
     * @param status      the new status of the task
     * @param newSchedule new time to execute the task (if status is scheduled)
     * @param retry       updated retry interval
     * @param message     message to communicate
     */
    @Override
    public CompletableFuture<Void> publishResult(String context, String taskId, TaskStatus status, Long newSchedule,
                                                 long retry, String message) {
        try {
            RaftGroupService raftGroupService = raftGroupServiceFactory
                    .getRaftGroupService(context);
            return raftGroupService.appendEntry(context, UpdateTask.class.getName(),
                                                UpdateTask.newBuilder()
                                                          .setTaskId(taskId)
                                                          .setStatus(status == null ? TaskStatus.FAILED : status)
                                                          .setInstant(Optional.of(newSchedule).orElse(0L))
                                                          .setRetryInterval(retry)
                                                          .setErrorMessage(message == null ? "" : message)
                                                          .build().toByteArray());
        } catch (Exception ex) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(ex);
            return completableFuture;
        }
    }
}
