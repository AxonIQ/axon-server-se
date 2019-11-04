package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.cluster.RaftGroupService;
import io.axoniq.axonserver.enterprise.cluster.RaftGroupServiceFactory;
import io.axoniq.axonserver.grpc.tasks.Status;
import io.axoniq.axonserver.grpc.tasks.UpdateTask;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;

/**
 * @author Marc Gathier
 */
@Component
public class TaskResultPublisherImpl implements TaskResultPublisher {

    private final RaftGroupServiceFactory raftGroupServiceFactory;

    public TaskResultPublisherImpl(RaftGroupServiceFactory raftGroupServiceFactory) {
        this.raftGroupServiceFactory = raftGroupServiceFactory;
    }

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
