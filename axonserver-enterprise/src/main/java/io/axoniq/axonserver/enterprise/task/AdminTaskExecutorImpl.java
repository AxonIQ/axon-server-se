package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.jpa.Task;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

/**
 * @author Marc Gathier
 * @since 4.3
 */
@Service
public class AdminTaskExecutorImpl implements AdminTaskExecutor {

    private final ApplicationContext applicationContext;
    private final TaskPayloadSerializer serializer;

    public AdminTaskExecutorImpl(ApplicationContext applicationContext,
                                 TaskPayloadSerializer serializer) {
        this.applicationContext = applicationContext;
        this.serializer = serializer;
    }


    @Override
    public void executeTask(Task task) {
        try {
            ScheduledJob job = (ScheduledJob) applicationContext.getBean(Class.forName(task.getTaskExecutor()));
            Object payload = serializer.deserialize(task.getPayload());
            job.execute(payload);
        } catch (ClassNotFoundException | BeansException ex) {
            throw new RuntimeException("Could not find handler for task as a Spring bean: " + task.getTaskExecutor(),
                                       ex);
        }
    }
}
