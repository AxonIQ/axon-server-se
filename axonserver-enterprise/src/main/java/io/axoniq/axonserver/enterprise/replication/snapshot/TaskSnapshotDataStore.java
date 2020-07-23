package io.axoniq.axonserver.enterprise.replication.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotDeserializationException;
import io.axoniq.axonserver.enterprise.taskscheduler.ClusterTaskManager;
import io.axoniq.axonserver.grpc.TaskStatus;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.taskscheduler.Task;
import reactor.core.publisher.Flux;

import java.util.List;


/**
 * Snapshot data store for {@link Task} data. This data is only distributed in the
 * _admin context.
 *
 * @author Marc Gathier
 * @since 4.1.5
 */
public class TaskSnapshotDataStore implements SnapshotDataStore {

    private static final String ENTRY_TYPE = ScheduleTask.class.getName();
    private final ClusterTaskManager taskManager;
    private final String replicationGroup;

    /**
     * Creates Context Snapshot Data Store for streaming/applying {@link ScheduleTask} data.
     *
     * @param replicationGroup the application context
     * @param taskManager      the task manager used for retrieving/saving tasks
     */
    public TaskSnapshotDataStore(String replicationGroup,
                                 ClusterTaskManager taskManager) {
        this.replicationGroup = replicationGroup;
        this.taskManager = taskManager;
    }

    @Override
    public int order() {
        return 50;
    }

    @Override
    public Flux<SerializedObject> streamSnapshotData(SnapshotContext installationContext) {
        List<Task> tasks = taskManager.findAllByReplicationGroup(replicationGroup);

        return Flux.fromIterable(tasks)
                   .filter(t -> TaskStatus.SCHEDULED.equals(t.getStatus()))
                   .map(this::asScheduleTask)
                   .map(this::toSerializedObject);
    }

    private ScheduleTask asScheduleTask(Task task) {
        return ScheduleTask.newBuilder()
                           .setRetryInterval(task.getRetryInterval())
                           .setInstant(task.getTimestamp())
                           .setTaskId(task.getTaskId())
                           .setTaskExecutor(task.getTaskExecutor())
                           .setPayload(task.getPayload().asSerializedObject())
                           .build();
    }

    @Override
    public boolean canApplySnapshotData(String type) {
        return ENTRY_TYPE.equals(type);
    }


    @Override
    public void applySnapshotData(SerializedObject serializedObject, Role role) {
        try {
            ScheduleTask scheduleTask = ScheduleTask
                    .parseFrom(serializedObject.getData());
            taskManager.schedule(replicationGroup, scheduleTask);
        } catch (InvalidProtocolBufferException e) {
            throw new SnapshotDeserializationException("Unable to deserialize application data.", e);
        }
    }

    @Override
    public void clear() {
        taskManager.deleteAllByReplicationGroup(replicationGroup);
    }

    private SerializedObject toSerializedObject(ScheduleTask scheduleTask) {
        return SerializedObject.newBuilder()
                               .setType(ENTRY_TYPE)
                               .setData(scheduleTask.toByteString())
                               .build();
    }
}
