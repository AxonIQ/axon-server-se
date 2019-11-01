package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.jpa.Task;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author Marc Gathier
 */
@Service
public class AdminTaskExecutor {

    private final ApplicationContext applicationContext;
    private final TaskPayloadSerializer serializer;

    public AdminTaskExecutor(ApplicationContext applicationContext,
                             TaskPayloadSerializer serializer) {
        this.applicationContext = applicationContext;
        this.serializer = serializer;
    }


    public void executeTask(Task task) throws IOException, ClassNotFoundException {
        ScheduledJob job = (ScheduledJob) applicationContext.getBean(Class.forName(task.getTaskExecutor()));
        Object payload = serializer.deserialize(task.getPayload());
        job.execute(payload);
    }
}
