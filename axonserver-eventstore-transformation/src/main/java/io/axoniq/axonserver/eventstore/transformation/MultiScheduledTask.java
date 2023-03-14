package io.axoniq.axonserver.eventstore.transformation;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class MultiScheduledTask implements TransformationTask {

    private final Iterable<TransformationTask> scheduledTasks;

    public MultiScheduledTask(Iterable<TransformationTask> scheduledTasks) {
        this.scheduledTasks = scheduledTasks;
    }

    @Override
    public void start() {
        scheduledTasks.forEach(TransformationTask::start);
    }

    @Override
    public void stop() {
        scheduledTasks.forEach(TransformationTask::stop);
    }
}
