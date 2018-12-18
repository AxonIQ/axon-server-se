package io.axoniq.axonserver.cluster.configuration.current;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Sara Pellegrini
 * @since
 */
public class RegisterConfigurationListener implements Function<Runnable, Registration> {

    private final Function<Consumer<Entry>, Registration> registerLogAppendListener;
    private final Function<Consumer<Entry>, Registration> registerLogRollbackListener;
    private final Function<Consumer<Entry>, Registration> registerLogAppliedListener;

    public RegisterConfigurationListener(RaftGroup raftGroup) {
        this(raftGroup.localLogEntryStore()::registerLogAppendListener,
             raftGroup.localLogEntryStore()::registerLogRollbackListener,
             raftGroup.logEntryProcessor()::registerLogAppliedListener);
    }

    public RegisterConfigurationListener(
            Function<Consumer<Entry>, Registration> registerLogAppendListener,
            Function<Consumer<Entry>, Registration> registerLogRollbackListener,
            Function<Consumer<Entry>, Registration> registerLogAppliedListener) {
        this.registerLogAppendListener = registerLogAppendListener;
        this.registerLogRollbackListener = registerLogRollbackListener;
        this.registerLogAppliedListener = registerLogAppliedListener;
    }

    @Override
    public Registration apply(Runnable runnable) {
        Consumer<Entry> onChange = entry -> {
            if (entry.hasNewConfiguration()){
                runnable.run();
            }
        };

        Registration append = registerLogAppendListener.apply(onChange);
        Registration rollback = registerLogRollbackListener.apply(onChange);
        Registration applied = registerLogAppliedListener.apply(onChange);

        return () -> {
            append.cancel();
            rollback.cancel();
            applied.cancel();
        };
    }
}
