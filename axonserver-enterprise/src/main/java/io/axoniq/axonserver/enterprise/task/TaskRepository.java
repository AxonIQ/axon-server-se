package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.jpa.Task;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 */
public interface TaskRepository extends JpaRepository<Task, Long> {

    List<Task> findAllByStatusNullAndTimestampBeforeOrderByTimestampAsc(long timestamp);

    Optional<Task> findFirstByTaskId(String taskId);

    default List<Task> findExecutableTasks(long timestamp) {
        return findAllByStatusNullAndTimestampBeforeOrderByTimestampAsc(timestamp);
    }
}
