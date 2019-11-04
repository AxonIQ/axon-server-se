package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.enterprise.jpa.Task;

import java.io.IOException;

/**
 * @author Marc Gathier
 */
public interface AdminTaskExecutor {

    void executeTask(Task task) throws IOException, ClassNotFoundException;
}
