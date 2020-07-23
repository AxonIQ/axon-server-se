package io.axoniq.axonserver.enterprise.taskscheduler;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.replication.group.RaftGroupServiceFactory;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.TaskStatus;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import io.axoniq.axonserver.taskscheduler.TaskPayload;
import io.axoniq.axonserver.taskscheduler.TaskPayloadSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Component to publish a new task to all nodes.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class TaskPublisher {

    private final Logger logger = LoggerFactory.getLogger(TaskPublisher.class);

    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final TaskPayloadSerializer taskPayloadSerializer;
    private final ApplicationContext applicationContext;

    public TaskPublisher(RaftGroupServiceFactory raftGroupServiceFactory,
                         TaskPayloadSerializer taskPayloadSerializer,
                         ApplicationContext applicationContext) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.taskPayloadSerializer = taskPayloadSerializer;
        this.applicationContext = applicationContext;
    }


    /**
     * Publishes a task to be executed with given payload after {@code delay}. Creates a Raft entry of with
     * type {@link ScheduleTask}, that will be stored in the control db upon applying the entry.
     *
     * @param context     the context in which to schedule the task
     * @param taskHandler the name of the class implementing the the task. There must be a Spring bean for this class.
     * @param payload     the payload to pass to the task upon execution.
     * @param delay       time to wait before executing the task
     * @return completable future containing the task id
     */
    public CompletableFuture<String> publishScheduledTask(String context, String taskHandler, Object payload,
                                                          Duration delay) {
        return publishScheduledTask(context, taskHandler, payload, System.currentTimeMillis() + delay.toMillis());
    }

    /**
     * Publishes a task to be executed with given payload at {@code instant}. Creates a Raft entry of with
     * type {@link ScheduleTask}, that will be stored in the control db upon applying the entry.
     *
     * @param context     the context in which to schedule the task
     * @param taskHandler the name of the class implementing the the task. There must be a Spring bean for this class.
     * @param payload     the payload to pass to the task upon execution.
     * @param instant     timestamp when to execute the task
     * @return completable future containing the task id
     */
    public CompletableFuture<String> publishScheduledTask(String context, String taskHandler, Object payload,
                                                          long instant) {
        try {
            SerializedObject payloadSerializedObject = null;
            if (payload instanceof SerializedObject) {
                payloadSerializedObject = (SerializedObject) payload;
            } else {
                TaskPayload serializedPayload = taskPayloadSerializer.serialize(payload);
                payloadSerializedObject = SerializedObject.newBuilder()
                                                          .setData(ByteString.copyFrom(serializedPayload
                                                                                               .getData()))
                                                          .setType(serializedPayload.getType())
                                                          .build();
            }
            ScheduleTask task = ScheduleTask.newBuilder()
                                            .setInstant(instant)
                                            .setPayload(payloadSerializedObject)
                                            .setTaskId(UUID.randomUUID().toString())
                                            .setTaskExecutor(taskHandler)
                                            .setRetryInterval(10)
                                            .build();

            logger.debug("Publish task {} with payload {}", taskHandler, task.getPayload().getType());

            return raftGroupServiceFactory.getRaftGroupService(context)
                                          .appendEntry(context, ScheduleTask.class.getName(), task.toByteArray())
                                          .thenApply(r -> task.getTaskId());
        } catch (Exception ex) {
            logger.info("{}: Publish task {} failed", context, taskHandler, ex);
            CompletableFuture<String> exceptionHolder = new CompletableFuture<>();
            exceptionHolder.completeExceptionally(ex);
            return exceptionHolder;
        }
    }

    /**
     * Publishes an action to cancel a scheduled task. Completes when the action is distributed and executed on
     * the leader of the context.
     *
     * @param context the context where the task is defined
     * @param taskId  the task identification
     * @return completable future that completes when task is cancelled on leader
     */
    public CompletableFuture<Void> cancelScheduledTask(String context, String taskId) {
        try {
            UpdateTask updateTask = UpdateTask.newBuilder()
                                              .setTaskId(taskId)
                                              .setInstant(0)
                                              .setRetryInterval(0)
                                              .setStatus(TaskStatus.CANCELLED)
                                              .build();

            return raftGroupServiceFactory.getRaftGroupService(context).appendEntry(context, UpdateTask.class.getName(),
                                                                                    updateTask.toByteArray());
        } catch (Exception ex) {
            CompletableFuture<Void> exceptionHolder = new CompletableFuture<>();
            exceptionHolder.completeExceptionally(ex);
            return exceptionHolder;
        }
    }
}
