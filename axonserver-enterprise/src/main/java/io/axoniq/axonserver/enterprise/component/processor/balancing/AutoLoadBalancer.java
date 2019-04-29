package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.grpc.ClientEventProcessorStatusProtoConverter;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Automatically balance the load between client application any time it is needed.
 *
 * @author Sara Pellegrini
 */
@Component
public class AutoLoadBalancer {

    private final Consumer<TrackingEventProcessor> balancer;

    private final Function<String,Boolean> coordinatorForContext;

    private final Map<TrackingEventProcessor, Collection<String>> cache = new ConcurrentHashMap<>();

    @Autowired
    public AutoLoadBalancer(UpdatedLoadBalance balancer, RaftLeaderProvider raftLeaderProvider) {
        this(balancer::balance, raftLeaderProvider::isLeader);
    }

    AutoLoadBalancer(Consumer<TrackingEventProcessor> balancer,
                     Function<String, Boolean> coordinatorForContext) {
        this.balancer = balancer;
        this.coordinatorForContext = coordinatorForContext;
    }

    @EventListener
    public void onEventProcessorStatusChange(EventProcessorEvents.EventProcessorStatusUpdated event) {
        ClientEventProcessorStatus status = ClientEventProcessorStatusProtoConverter.toProto(event.eventProcessorStatus());
        String context = status.getContext();
        String client = status.getClient();
        String processor = status.getEventProcessorInfo().getProcessorName();
        TrackingEventProcessor current = new TrackingEventProcessor(processor, context);
        Collection<String> clients = cache.computeIfAbsent(current, s -> new LinkedList<>());

        if (!clients.contains(client)) {
            clients.add(client);
            balance(current);
        }
    }

    @EventListener
    public void onClientDisconnected(TopologyEvents.ApplicationDisconnected event) {
        cache.forEach((processor, clients) -> {
            boolean removed = clients.remove(event.getClient());
            if (removed){
                balance(processor);
            }
        });
    }

    private void balance(TrackingEventProcessor processor) {
        if (isNotCoordinator(processor.context())) return;
        balancer.accept(processor);
    }

    private boolean isNotCoordinator(String context){
        return !coordinatorForContext.apply(context);
    }

}
