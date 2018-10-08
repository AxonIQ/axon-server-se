package io.axoniq.axonserver.rest.svg.mapping;

import io.axoniq.axonserver.topology.AxonServerNode;

import java.util.Collections;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeAxonServer implements AxonServer {

    private final boolean active;
    private final AxonServerNode node;
    private final Set<String> contexts;
    private final Set<String> disconnectedContexts;

    public FakeAxonServer(boolean active, AxonServerNode node, Set<String> contexts,
                          Set<String> disconnectedContexts) {
        this.active = active;
        this.node = node;
        this.contexts = contexts;
        this.disconnectedContexts = disconnectedContexts;
    }


    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public AxonServerNode node() {
        return node;
    }

    @Override
    public Set<String> contexts() {
        return contexts;
    }

    @Override
    public Set<Storage> storage() {
        return Collections.emptySet();
    }
}
