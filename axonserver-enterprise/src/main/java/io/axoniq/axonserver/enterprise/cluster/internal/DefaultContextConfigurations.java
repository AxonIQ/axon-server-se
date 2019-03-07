package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterable of all context configurations persisted in controlDB.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@Component
public class DefaultContextConfigurations implements ContextConfigurations {

    private final ContextController contextController;
    private final Function<Context, ContextConfiguration> mapping;

    @Autowired
    public DefaultContextConfigurations(ContextController contextController) {
        this(contextController, new ContextConfigurationMapping());
    }

    public DefaultContextConfigurations(ContextController contextController,
                                        Function<Context, ContextConfiguration> mapping) {
        this.contextController = contextController;
        this.mapping = mapping;
    }

    @NotNull
    @Override
    public Iterator<ContextConfiguration> iterator() {
        return contextController.getContexts().map(mapping).iterator();
    }
}

