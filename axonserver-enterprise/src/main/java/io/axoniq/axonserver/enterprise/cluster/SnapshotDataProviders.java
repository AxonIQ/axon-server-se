package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalanceStrategyRepository;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancingRepository;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ApplicationSnapshotDataProvider;
import io.axoniq.axonserver.enterprise.cluster.snapshot.EventTransactionsSnapshotDataProvider;
import io.axoniq.axonserver.enterprise.cluster.snapshot.LoadBalanceStrategySnapshotDataProvider;
import io.axoniq.axonserver.enterprise.cluster.snapshot.ProcessorLoadBalancingSnapshotDataProvider;
import io.axoniq.axonserver.enterprise.cluster.snapshot.SnapshotDataProvider;
import io.axoniq.axonserver.enterprise.cluster.snapshot.SnapshotTransactionsSnapshotDataProvider;
import io.axoniq.axonserver.enterprise.cluster.snapshot.UserSnapshotDataProvider;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.platform.application.ApplicationController;
import io.axoniq.platform.user.UserRepository;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;

/**
 * @author Sara Pellegrini
 * @since
 */
@Component
public class SnapshotDataProviders implements Function<String, List<SnapshotDataProvider>> {

    private final ApplicationController applicationController;

    private final UserRepository userRepository;

    private final LoadBalanceStrategyRepository loadBalanceStrategyRepository;

    private final ProcessorLoadBalancingRepository processorLoadBalancingRepository;

    private final ApplicationContext applicationContext;

//    private final LocalEventStore localEventStore;

    public SnapshotDataProviders(
            ApplicationController applicationController,
            UserRepository userRepository,
            LoadBalanceStrategyRepository loadBalanceStrategyRepository,
            ProcessorLoadBalancingRepository processorLoadBalancingRepository,
//                                 LocalEventStore localEventStore
            ApplicationContext applicationContext) {
        this.applicationController = applicationController;
        this.userRepository = userRepository;
        this.loadBalanceStrategyRepository = loadBalanceStrategyRepository;
        this.processorLoadBalancingRepository = processorLoadBalancingRepository;
//        this.localEventStore = localEventStore;
        this.applicationContext = applicationContext;
    }

    public List<SnapshotDataProvider> apply(String context){
        LocalEventStore localEventStore = applicationContext.getBean(LocalEventStore.class);
        return asList(new ApplicationSnapshotDataProvider(context, applicationController),
                      new EventTransactionsSnapshotDataProvider(context, localEventStore),
                      new LoadBalanceStrategySnapshotDataProvider(loadBalanceStrategyRepository),
                      new ProcessorLoadBalancingSnapshotDataProvider(context, processorLoadBalancingRepository),
                      new SnapshotTransactionsSnapshotDataProvider(context, localEventStore),
                      new UserSnapshotDataProvider(userRepository));
    }

}
