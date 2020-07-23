package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.taskscheduler.StandaloneTaskManager;
import io.axoniq.axonserver.taskscheduler.Task;
import io.axoniq.axonserver.taskscheduler.TaskRepository;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 * @since 4.3
 */
@RestController
public class TaskRestController {

    private static final Logger auditLog = AuditLog.getLogger();
    private final TaskRepository taskRepository;
    private final TaskPublisher taskPublisher;
    private final StandaloneTaskManager standaloneTaskManager;


    public TaskRestController(TaskRepository taskRepository,
                              TaskPublisher taskPublisher,
                              StandaloneTaskManager standaloneTaskManager) {
        this.taskRepository = taskRepository;
        this.taskPublisher = taskPublisher;
        this.standaloneTaskManager = standaloneTaskManager;
    }

    @GetMapping(path = "/v1/tasks")
    public List<Task> list(Principal principal) {
        auditLog.info("[{}] Request to list tasks.",
                      AuditLog.username(principal));
        return taskRepository.findAll();
    }

    @DeleteMapping(path = "/v1/tasks/{taskId}")
    public void delete(@PathVariable("taskId") String taskId, Principal principal) {
        auditLog.info("[{}] Request to delete task {}.",
                      AuditLog.username(principal), taskId);
        Optional<Task> task = taskRepository.findById(taskId);
        task.ifPresent(t -> {
            if (t.getContext().equals("_local")) {
                standaloneTaskManager.cancel(taskId);
            } else {
                taskPublisher.cancelScheduledTask(t.getContext(), t.getTaskId());
            }
        });
    }
}
