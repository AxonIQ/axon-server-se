package io.axoniq.axonserver.cluster.configuration;

import java.util.concurrent.CompletableFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface WaitStrategy {

    CompletableFuture<Void> await();

}
