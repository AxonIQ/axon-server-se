package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultTransformationCancelTask implements TransformationCancelTask {

    private final TransformationCancelExecutor executor;
    private final Transformers transformers;
    private final Transformations transformations;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public DefaultTransformationCancelTask(TransformationCancelExecutor executor,
                                           Transformers transformers,
                                           Transformations transformations) {
        this.executor = executor;
        this.transformers = transformers;
        this.transformations = transformations;
    }

    @Override
    public void start() {
        scheduledExecutorService.schedule(this::cancel, 10, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
    }

    private void cancel() {
        transformations.cancellingTransformations()
                       .flatMap(transformation -> executor.cancel(new TransformationCancelExecutor.Transformation() {
                           @Override
                           public String id() {
                               return transformation.id();
                           }

                           @Override
                           public String context() {
                               return transformation.context();
                           }

                           @Override
                           public int version() {
                               return transformation.version();
                           }

                           @Override
                           public Mono<Void> markAsCancelled() {
                               return transformers.transformerFor(context())
                                                  .flatMap(t -> t.markAsCancelled(id()));
                           }
                       }))
                       .subscribe(/* TODO logger */);
    }
}
