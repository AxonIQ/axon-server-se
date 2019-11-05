package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.cluster.RaftGroupService;
import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.grpc.tasks.Status;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

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
     * @param taskId      the unique id of the task
     * @param status      the new status of the task
     * @param newSchedule new time to execute the task (if status is scheduled)
     * @param retry       updated retry interval
     */
    public void publishResult(String taskId, Status status, Long newSchedule, long retry) {
        RaftGroupService raftGroupService = raftGroupServiceFactory
                .getRaftGroupService(getAdmin());
        raftGroupService.appendEntry(getAdmin(), UpdateTask.class.getName(),
                                     UpdateTask.newBuilder()
                                               .setTaskId(taskId)
                                               .setStatus(status == null ? Status.FAILED : status)
                                               .setInstant(Optional.of(newSchedule).orElse(0L))
                                               .setRescheduleAfter(retry)
                                               .build().toByteArray());
    }
}
