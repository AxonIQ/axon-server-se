package io.axoniq.axonserver.enterprise.task;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author Marc Gathier
 */
public interface ScheduledJob {

    void execute(Object payload) throws JsonProcessingException;
}
