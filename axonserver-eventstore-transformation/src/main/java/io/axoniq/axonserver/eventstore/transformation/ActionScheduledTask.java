package io.axoniq.axonserver.eventstore.transformation;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class ActionScheduledTask implements TransformationTask {

    private final AtomicReference<Disposable> disposable = new AtomicReference<>();
    private final AtomicReference<ScheduledExecutorService> scheduledExecutorService = new AtomicReference<>();
    private final Mono<Void> action;

    public ActionScheduledTask(ActionSupplier actionSupplier) {
        this(actionSupplier.get());
    }

    public ActionScheduledTask(Mono<Void> action) {
        this.action = action;
    }

    @Override
    public void start() {
        if (scheduledExecutorService.compareAndSet(null, Executors.newSingleThreadScheduledExecutor())) {
            scheduledExecutorService.get().schedule(() -> disposable.compareAndSet(null, apply()),
                                                    10, TimeUnit.SECONDS);
        }
    }

    @Override
    public void stop() {
        Disposable task = disposable.getAndSet(null);
        if (task != null) {
            task.dispose();
        }
        ScheduledExecutorService service = scheduledExecutorService.getAndSet(null);
        if (service != null) {
            service.shutdownNow();
        }
    }

    private Disposable apply() {
        return action
                .doFinally(s -> scheduledExecutorService.get()
                                                        .schedule(() -> disposable.set(apply()), 10, TimeUnit.SECONDS))
                .subscribe();
    }
}
