package io.axoniq.axonserver.cluster.configuration.current;

import io.axoniq.axonserver.cluster.CurrentConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CachedCurrentConfiguration implements CurrentConfiguration {

    private final CurrentConfiguration currentConfiguration;

    private final AtomicReference<List<Node>> cachedNodes = new AtomicReference<>();

    private final AtomicBoolean cachedIsUncommitted = new AtomicBoolean();

    private final List<Consumer<List<Node>>> listeners = new CopyOnWriteArrayList<>();

    public CachedCurrentConfiguration(RaftGroup raftGroup) {
        this(new DefaultCurrentConfiguration(raftGroup),
             new RegisterConfigurationListener(raftGroup));
    }

    public CachedCurrentConfiguration(CurrentConfiguration currentConfiguration,
                                      Function<Runnable, Registration> registerConfigurationListener) {
        this.currentConfiguration = currentConfiguration;
        registerConfigurationListener.apply(this::update);
    }

    @Override
    public List<Node> groupMembers() {
        if (cachedNodes.get() == null){
            update();
        }
        return cachedNodes.get();
    }

    @Override
    public boolean isUncommitted() {
        if (cachedNodes.get() == null){
            update();
        }
        return cachedIsUncommitted.get();
    }

    private void update(){
        List<Node> newConfig = currentConfiguration.groupMembers();
        List<Node> oldConfig = cachedNodes.getAndSet(newConfig);
        cachedIsUncommitted.set(currentConfiguration.isUncommitted());
        if (!newConfig.equals(oldConfig)){
            listeners.forEach(listener -> listener.accept(newConfig));
        }
    }

    public Registration registerChangeListener(Consumer<List<Node>> newConfigurationConsumer){
        listeners.add(newConfigurationConsumer);
        return () -> listeners.remove(newConfigurationConsumer);
    }
}
