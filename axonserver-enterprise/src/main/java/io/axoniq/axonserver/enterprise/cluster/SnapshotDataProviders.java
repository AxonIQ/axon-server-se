package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.JpaContextApplicationController;
import io.axoniq.axonserver.access.user.UserRepository;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ApplicationSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ContextApplicationSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ContextSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.EventTransactionsSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ProcessorLoadBalancingSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.SnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.SnapshotTransactionsSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.UserSnapshotDataStore;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * Providers of the list of {@link SnapshotDataStore}s needed to replicate the full state.
 * @author Sara Pellegrini
 * @since 4.1
 */
@Component
public class SnapshotDataProviders implements Function<String, List<SnapshotDataStore>> {

    private final ApplicationController applicationController;

    private final UserRepository userRepository;

    private final ProcessorLoadBalancingRepository processorLoadBalancingRepository;

    private final JpaContextApplicationController contextApplicationController;
    private final ContextController contextController;
    private final ApplicationContext applicationContext;

    public SnapshotDataProviders(
            ApplicationController applicationController,
            UserRepository userRepository,
            ProcessorLoadBalancingRepository processorLoadBalancingRepository,
            JpaContextApplicationController contextApplicationController,
            ContextController contextController,
            ApplicationContext applicationContext) {
        this.applicationController = applicationController;
        this.userRepository = userRepository;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
        this.contextApplicationController = contextApplicationController;
        this.contextController = contextController;
        this.applicationContext = applicationContext;
    }

    /**
     * Returns the list of {@link SnapshotDataStore}s needed to replicate the full state for the specified context.
     * @param context the context
     * @return the list of {@link SnapshotDataStore}s
     */
    public List<SnapshotDataStore> apply(String context){
        LocalEventStore localEventStore = applicationContext.getBean(LocalEventStore.class);
        return asList(
                new ContextSnapshotDataStore(context, contextController),
                new ApplicationSnapshotDataStore(context, applicationController),
                      new ContextApplicationSnapshotDataStore(context, contextApplicationController),
                      new EventTransactionsSnapshotDataStore(context, localEventStore),
                      new ProcessorLoadBalancingSnapshotDataStore(context, processorLoadBalancingRepository),
                      new SnapshotTransactionsSnapshotDataStore(context, localEventStore),
                      new UserSnapshotDataStore(context, userRepository));
    }

}
