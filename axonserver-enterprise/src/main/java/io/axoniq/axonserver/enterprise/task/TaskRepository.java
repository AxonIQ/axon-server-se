package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.jpa.Task;
import io.axoniq.axonserver.grpc.tasks.Status;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * Repository of tasks to execute on the admin leader.
 * @author Marc Gathier
 * @since 4.3
 */
public interface TaskRepository extends JpaRepository<Task, Long> {

    /**
     * Finds tasks based on their status and maximum timestamp
     *
     * @param status    the status of the task
     * @param timestamp the max timestamp of the task
     * @return list of matching tasks
     */
    List<Task> findAllByStatusAndTimestampBeforeOrderByTimestampAsc(Status status, long timestamp);

    /**
     * Tries to find a task based on its id.
     * @param taskId the id of the task
     * @return optional task
     */
    Optional<Task> findFirstByTaskId(String taskId);

    /**
     * Utility to find tasks that can be executed at {@code timestamp}.
     * @param timestamp the max timestamp of the tasks
     * @return list of executable tasks at given timestamp
     */
    default List<Task> findExecutableTasks(long timestamp) {
        return findAllByStatusAndTimestampBeforeOrderByTimestampAsc(Status.SCHEDULED, timestamp);
    }
}
