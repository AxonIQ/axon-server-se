package io.axoniq.axonserver.enterprise.taskscheduler;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Component to publish a new task to all nodes.
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class TaskPublisher {

    private final Logger logger = LoggerFactory.getLogger(TaskPublisher.class);

    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final TaskPayloadSerializer taskPayloadSerializer;

    public TaskPublisher(RaftGroupServiceFactory raftGroupServiceFactory,
                         TaskPayloadSerializer taskPayloadSerializer) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.taskPayloadSerializer = taskPayloadSerializer;
    }


    /**
     * Publishes a task to be executed with given payload after {@code delay} milliseconds. Creates a Raft entry of with
     * type {@link ScheduleTask}, that
     * will be stored in the control db upon applying the entry.
     *
     * @param context     the context in which to schedule the task
     * @param taskHandler the name of the class implementing the the task. There must be a Spring bean for this class.
     * @param payload     the payload to pass to the task upon execution.
     * @param delay       time to wait before executing the task
     */
    public CompletableFuture<Void> publishScheduledTask(String context, String taskHandler, Object payload,
                                                        Duration delay) {
        Payload serializedPayload = taskPayloadSerializer.serialize(payload);
        ScheduleTask task = ScheduleTask.newBuilder()
                                        .setInstant(System.currentTimeMillis() + delay.toMillis())
                                        .setPayload(SerializedObject.newBuilder()
                                                                    .setData(ByteString.copyFrom(serializedPayload
                                                                                                         .getData()))
                                                                    .setType(serializedPayload.getType())
                                                                    .build())
                                        .setTaskId(UUID.randomUUID().toString())
                                        .setTaskExecutor(taskHandler)
                                        .build();

        logger.debug("Publish task {} with payload {}", taskHandler, task.getPayload().getData().toStringUtf8());

        return raftGroupServiceFactory.getRaftGroupService(context).appendEntry(context,
                                                                                ScheduleTask.class.getName(),
                                                                                task.toByteArray());
    }
}
