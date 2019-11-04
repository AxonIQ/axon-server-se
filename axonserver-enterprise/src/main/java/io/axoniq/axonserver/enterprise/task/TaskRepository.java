package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.tasks.Status;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface TaskRepository extends JpaRepository<Task, Long> {

    List<Task> findAllByStatusAndTimestampBeforeOrderByTimestampAsc(Status status, long timestamp);

    Optional<Task> findFirstByTaskId(String taskId);

    default List<Task> findExecutableTasks(long timestamp) {
        return findAllByStatusAndTimestampBeforeOrderByTimestampAsc(Status.SCHEDULED, timestamp);
    }
}
