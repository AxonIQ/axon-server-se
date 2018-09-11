package io.axoniq.axonserver.enterprise.cluster.coordinator;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 22/08/2018.
 * sara.pellegrini@gmail.com
 */
@Controller
public class AxonHubManager {

    private final Logger logger = LoggerFactory.getLogger(AxonHubManager.class);

    private final String thisNodeName;

    private final Boolean clusterEnabled;

    private final Iterable<Context> dynamicContexts;

    private final ApplicationEventPublisher eventPublisher;

    private final CoordinatorElectionProcess electionProcess;

    private final Map<String, String> coordinatorPerContext = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private volatile ScheduledFuture<?> task;


    @Autowired
    public AxonHubManager(MessagingPlatformConfiguration messagingPlatformConfiguration,
                          ContextController contextController,
                          CoordinatorElectionProcess coordinatorElectionProcess,
                          ApplicationEventPublisher applicationEventPublisher) {
        this(
                messagingPlatformConfiguration.getName(),
                messagingPlatformConfiguration.getCluster().isEnabled(),
                () -> contextController.getContexts().iterator(),
                applicationEventPublisher,
                coordinatorElectionProcess);
    }

    public AxonHubManager(String thisNodeName,
                          Boolean clusterEnabled,
                          Iterable<Context> dynamicContexts,
                          ApplicationEventPublisher applicationEventPublisher,
                          CoordinatorElectionProcess electionProcess) {
        this.thisNodeName = thisNodeName;
        this.clusterEnabled = clusterEnabled;
        this.dynamicContexts = dynamicContexts;
        this.eventPublisher = applicationEventPublisher;
        this.electionProcess = electionProcess;
    }

    public String coordinatorFor(String context) {
        return coordinatorPerContext.get(context);
    }

    public boolean isCoordinatorFor(String context) {
        return isCoordinator(thisNodeName, context);
    }

    public boolean isCoordinator(String nodeName, String context) {
        String coordinator = coordinatorPerContext.get(context);
        return coordinator != null && coordinator.equals(nodeName);
    }

    @PostConstruct
    public void init() {
        dynamicContexts.forEach(this::initContext);
    }

    @EventListener
    public void on(ContextEvents.ContextCreated event) {
        initContext(event.getName());
    }

    @EventListener
    public void on(ContextEvents.NodeAddedToContext event) {
        initContext(event.getName());
    }

    @EventListener
    public void on(ClusterEvents.CoordinatorConfirmation event) {
        this.coordinatorPerContext.put(event.context(), event.node());
    }

    @EventListener
    public void on(ClusterEvents.BecomeCoordinator event) {
        this.coordinatorPerContext.put(event.context(), event.node());
    }

    @EventListener
    public void on(ClusterEvents.AxonHubInstanceDisconnected event) {
        contextsCoordinatedBy(event.getNodeName()).forEach(context -> {
            eventPublisher.publishEvent(new ClusterEvents.CoordinatorStepDown(context, false));
        });
    }

    @EventListener
    public void on(ContextEvents.NodeDeletedFromContext event) {
        if (isCoordinatorFor(event.getName())) {
            eventPublisher.publishEvent(new ClusterEvents.CoordinatorStepDown(event.getName(), false));
        }
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted event) {
        if (isCoordinatorFor(event.getName())) {
            eventPublisher.publishEvent(new ClusterEvents.CoordinatorStepDown(event.getName(), false));
        }
    }

    @EventListener
    public void on(ClusterEvents.CoordinatorStepDown event) {
        coordinatorPerContext.remove(event.context());
        scheduleCoordinationElection(event.context());
    }

    private void scheduleCoordinationElection(String context) {
        if (task == null || task.isDone()) {
            task = scheduledExecutorService.schedule(() -> {
                electionProcess.startElection(context(context), () -> coordinatorPerContext.containsKey(context));
            }, 0, TimeUnit.SECONDS);
        }
    }

    Iterable<String> contextsCoordinatedBy(String nodeName) {
        return () -> coordinatorPerContext.entrySet().stream()
                                          .filter(e -> e.getValue().equals(nodeName))
                                          .map(Map.Entry::getKey)
                                          .iterator();
    }

    private void initContext(String name) {
        initContext(context(name));
    }

    private void initContext(Context context) {
        if (!context.isMessagingMember(thisNodeName)) {
            return;
        }
        if (!clusterEnabled) {
            coordinatorPerContext.put(context.getName(), thisNodeName);
            return;
        }
        logger.debug("Scheduling initial coordinator election");
        scheduleCoordinationElection(context.getName());
    }

    private Context context(String context) {
        return contextsStream().filter(c -> context.equals(c.getName())).findFirst()
                               .orElseThrow(() -> new IllegalArgumentException(context + " context doesn't exist"));
    }

    private Stream<Context> contextsStream() {
        return stream(dynamicContexts.spliterator(), false);
    }
}
