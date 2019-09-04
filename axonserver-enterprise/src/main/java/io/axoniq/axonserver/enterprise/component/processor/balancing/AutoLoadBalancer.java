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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Automatically balance the load between client applications any time it is needed.
 * In this implementation, the balance operation is performed only if the node instance is the current leader for the context.
 * @author Sara Pellegrini
 * @since 4.0
 *
 */
@Component
public class AutoLoadBalancer {

    private final Consumer<TrackingEventProcessor> balancer;

    private final Function<String,Boolean> coordinatorForContext;

    private final Map<TrackingEventProcessor, Map<String, Integer>> cache = new ConcurrentHashMap<>();

    @Autowired
    public AutoLoadBalancer(LoadBalancerDelegate balancer, RaftLeaderProvider raftLeaderProvider) {
        this(balancer::balance, raftLeaderProvider::isLeader);
    }

    AutoLoadBalancer(Consumer<TrackingEventProcessor> balancer,
                     Function<String, Boolean> coordinatorForContext) {
        this.balancer = balancer;
        this.coordinatorForContext = coordinatorForContext;
    }

    /**
     * Listens to any update of the event processors status and balances the load if this node is the current leader and
     * there is a change in the number of active threads on the client (or processor/client was unknown).
     *
     * @param event the internal event describing the new status of the event processor
     */
    @EventListener
    public void onEventProcessorStatusChange(EventProcessorEvents.EventProcessorStatusUpdated event) {
        ClientEventProcessorStatus status = ClientEventProcessorStatusProtoConverter
                .toProto(event.eventProcessorStatus());
        String context = status.getContext();
        String client = status.getClient();
        String processor = status.getEventProcessorInfo().getProcessorName();
        Integer newActiveThreads = status.getEventProcessorInfo().getActiveThreads();
        TrackingEventProcessor trackingEventProcessor = new TrackingEventProcessor(processor, context);
        Integer previousActiveThreads = cache.computeIfAbsent(trackingEventProcessor, s -> new ConcurrentHashMap<>())
                                             .put(client, newActiveThreads);

        if (!newActiveThreads.equals(previousActiveThreads)) {
            balance(trackingEventProcessor);
        }
    }
    /**
     *  Listens to any client disconnected and balances the load of its event processors if this node is the current leader.
     * @param event the internal event describing the client application disconnected from the Axon Server cluster
     */
    @EventListener
    public void onClientDisconnected(TopologyEvents.ApplicationDisconnected event) {
        cache.forEach((processor, clients) -> {
            Integer removed = clients.remove(event.getClient());
            if (removed != null) {
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
