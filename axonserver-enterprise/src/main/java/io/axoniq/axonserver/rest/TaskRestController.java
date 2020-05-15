package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.taskscheduler.Task;
import io.axoniq.axonserver.taskscheduler.TaskRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author Marc Gathier
 * @since 4.3
 */
@RestController
public class TaskRestController {

    private final TaskRepository taskRepository;


    public TaskRestController(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @GetMapping(path = "/v1/tasks")
    public List<Task> list() {
        return taskRepository.findAll();
    }
}
