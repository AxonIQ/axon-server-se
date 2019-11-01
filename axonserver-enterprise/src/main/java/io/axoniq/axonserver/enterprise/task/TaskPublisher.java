package io.axoniq.axonserver.enterprise.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.jpa.Payload;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * @author Marc Gathier
 */
@Component
public class TaskPublisher {

    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final TaskPayloadSerializer taskPayloadSerializer;

    public TaskPublisher(RaftGroupServiceFactory raftGroupServiceFactory,
                         TaskPayloadSerializer taskPayloadSerializer) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.taskPayloadSerializer = taskPayloadSerializer;
    }


    public void publishTask(String taskHandler, Object payload, long delay) throws JsonProcessingException {
        Payload serializedPayload = taskPayloadSerializer.serialize(payload);
        ScheduleTask task = ScheduleTask.newBuilder()
                                        .setInstant(System.currentTimeMillis() + delay)
                                        .setPayload(SerializedObject.newBuilder()
                                                                    .setData(ByteString.copyFrom(serializedPayload
                                                                                                         .getData()))
                                                                    .setType(serializedPayload.getType())
                                                                    .build())
                                        .setTaskId(UUID.randomUUID().toString())
                                        .setTaskExecutor(taskHandler)
                                        .build();

        System.out.println(taskHandler + " => " + task.getPayload().getData().toStringUtf8());

        raftGroupServiceFactory.getRaftGroupService(getAdmin()).appendEntry(getAdmin(),
                                                                            ScheduleTask.class.getName(),
                                                                            task.toByteArray());
    }
}
