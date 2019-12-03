package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;

/**
 * @author Marc Gathier
 */
@KeepNames
public class AddNodeToContext {

    private String context;

    public AddNodeToContext() {
    }

    public AddNodeToContext(String context) {
        this.context = context;
    }

    public String getContext() {
        return context;
    }
}
