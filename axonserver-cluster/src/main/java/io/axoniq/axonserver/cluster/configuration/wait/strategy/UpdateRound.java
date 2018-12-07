package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.configuration.WaitStrategy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class UpdateRound implements WaitStrategy {

    private final Supplier<Long> lastIndex;

    private final Function<Consumer<Long>, Registration> registerMatchIndexListener;

    public UpdateRound(Supplier<Long> lastIndex,
                       Function<Consumer<Long>, Registration> registerMatchIndexListener) {
        this.lastIndex = lastIndex;
        this.registerMatchIndexListener = registerMatchIndexListener;
    }

    public CompletableFuture<Void> await(){
        long stopRoundAt = lastIndex.get();
        CompletableFuture<Void> roundCompleted = new CompletableFuture<>();
        Registration registration = registerMatchIndexListener.apply(matchIndex -> {
            if (matchIndex >= stopRoundAt) {
                roundCompleted.complete(null);
            }
        });
        roundCompleted.thenRun(registration::cancel);
        return roundCompleted;
    }

}
