package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.Disposable;
import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
@FunctionalInterface
public interface NodeReplicator {

    Disposable start(Consumer<Long> matchIndexCallback);

}
