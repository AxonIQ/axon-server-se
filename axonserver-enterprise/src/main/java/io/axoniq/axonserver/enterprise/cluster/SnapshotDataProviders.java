package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.user.UserRepository;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ApplicationSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.EventTransactionsSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.LoadBalanceStrategySnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ProcessorLoadBalancingSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.SnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.SnapshotTransactionsSnapshotDataStore;
import io.axoniq.axonserver.enterprise.cluster.snapshot.UserSnapshotDataStore;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
@Component
public class SnapshotDataProviders implements Function<String, List<SnapshotDataStore>> {

    private final ApplicationController applicationController;

    private final UserRepository userRepository;

    private final LoadBalanceStrategyRepository loadBalanceStrategyRepository;

    private final ProcessorLoadBalancingRepository processorLoadBalancingRepository;

    private final ApplicationContext applicationContext;

    public SnapshotDataProviders(
            ApplicationController applicationController,
            UserRepository userRepository,
            LoadBalanceStrategyRepository loadBalanceStrategyRepository,
            ProcessorLoadBalancingRepository processorLoadBalancingRepository,
            ApplicationContext applicationContext) {
        this.applicationController = applicationController;
        this.userRepository = userRepository;
        this.loadBalanceStrategyRepository = loadBalanceStrategyRepository;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
        this.applicationContext = applicationContext;
    }

    public List<SnapshotDataStore> apply(String context){
        LocalEventStore localEventStore = applicationContext.getBean(LocalEventStore.class);
        return asList(new ApplicationSnapshotDataStore(context, applicationController),
                      new EventTransactionsSnapshotDataStore(context, localEventStore),
                      new LoadBalanceStrategySnapshotDataStore(loadBalanceStrategyRepository),
                      new ProcessorLoadBalancingSnapshotDataStore(context, processorLoadBalancingRepository),
                      new SnapshotTransactionsSnapshotDataStore(context, localEventStore),
                      new UserSnapshotDataStore(userRepository));
    }

}
