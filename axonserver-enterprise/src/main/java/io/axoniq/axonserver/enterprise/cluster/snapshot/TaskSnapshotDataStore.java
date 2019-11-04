package io.axoniq.axonserver.enterprise.cluster.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.enterprise.task.TaskRepository;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;


/**
 * Snapshot data store for {@link io.axoniq.axonserver.enterprise.jpa.Task} data. This data is only distributed in the
 * _admin context.
 *
 * @author Marc Gathier
 * @since 4.1.5
 */
public class TaskSnapshotDataStore implements SnapshotDataStore {

    public static final String ENTRY_TYPE = ScheduleTask.class.getName();
    private final TaskRepository taskRepository;
    private final boolean adminContext;

    /**
     * Creates Context Snapshot Data Store for streaming/applying {@link Context} data.
     *
     * @param context        the application context
     * @param taskRepository the repository used for retrieving/saving tasks
     */
    public TaskSnapshotDataStore(String context, TaskRepository taskRepository) {
        this.adminContext = isAdmin(context);
        this.taskRepository = taskRepository;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        if (!adminContext) {
            return Flux.empty();
        }
        List<Task> tasks = taskRepository.findAll();

        return Flux.fromIterable(tasks)
                   .map(t -> t.asScheduleTask())
                   .map(this::toSerializedObject);
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return adminContext && ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject) {
        try {
            ScheduleTask scheduleTask = ScheduleTask
                    .parseFrom(serializedObject.getData());
            taskRepository.save(new Task(scheduleTask));
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        if (adminContext) {
            taskRepository.deleteAll();
        }
    }

    private SerializedObject toSerializedObject(ScheduleTask scheduleTask) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(scheduleTask.toByteString())
                               .build();
    }
}
