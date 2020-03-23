package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.RaftGroupRepositoryManager;
import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.messaging.event.RemoteEventStore;
import io.axoniq.axonserver.grpc.ChannelProvider;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.topology.EventStoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;

import java.util.function.BiFunction;
import java.util.function.Function;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 */
@KeepNames
public class EventStoreManager implements SmartLifecycle, EventStoreLocator {

    private final Logger logger = LoggerFactory.getLogger(EventStoreManager.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final LifecycleController lifecycleController;
    private final LocalEventStore localEventStore;
    private final ChannelProvider channelProvider;
    private volatile boolean running;

    private final Iterable<String> dynamicContexts;
    private final BiFunction<String, Boolean, String> leaderProvider;
    private final boolean needsValidation;
    private final String nodeName;
    private final Function<String, ClusterNode> clusterNodeSupplier;
    private final RaftGroupRepositoryManager contextController;

    public EventStoreManager(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             LifecycleController lifecycleController,
                             LocalEventStore localEventStore,
                             ChannelProvider channelProvider,
                             Iterable<String> dynamicContexts,
                             BiFunction<String, Boolean, String> leaderProvider,
                             boolean needsValidation, String nodeName,
                             Function<String, ClusterNode> clusterNodeSupplier,
                             RaftGroupRepositoryManager contextController) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.lifecycleController = lifecycleController;
        this.localEventStore = localEventStore;
        this.channelProvider = channelProvider;
        this.dynamicContexts = dynamicContexts;
        this.leaderProvider = leaderProvider;
        this.needsValidation = needsValidation;
        this.nodeName = nodeName;
        this.clusterNodeSupplier = clusterNodeSupplier;
        this.contextController = contextController;
    }

    @Autowired
    public EventStoreManager(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             ClusterController clusterController,
                             LifecycleController lifecycleController,
                             RaftLeaderProvider leaderProvider,
                             RaftGroupRepositoryManager contextController,
                             LocalEventStore localEventStore,
                             ChannelProvider channelProvider) {
        this(messagingPlatformConfiguration,
             lifecycleController,
             localEventStore,
             channelProvider,
             () -> contextController.storageContexts().iterator(),
             leaderProvider::getLeaderOrWait,
             !lifecycleController.isCleanShutdown(),
             messagingPlatformConfiguration.getName(),
             clusterController::getNode,
             contextController);
    }


    public void start() {
        try {
            dynamicContexts.forEach(context -> initContext(context, needsValidation));
        } catch (RuntimeException t) {
            logger.error("Failed to start storage: {}", t.getMessage(), t);
            lifecycleController.abort();
        }
        lifecycleController.setCleanShutdown();
        running = true;
    }

    @EventListener
    public void on(ClusterEvents.LeaderStepDown leaderStepDown) {
        localEventStore.cancel(leaderStepDown.getContextName());
    }

    @EventListener
    public void on(ClusterEvents.BecomeLeader becomeLeader) {
        if (!isAdmin(becomeLeader.getContext())) {
            initContext(becomeLeader.getContext(), false);
        }
    }

    @EventListener
    public void on(ContextEvents.ContextCreated contextCreated) {
        initContext(contextCreated.getContext(), false);
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        logger.info("{}: close context", contextDeleted.getContext());
        localEventStore.deleteContext(contextDeleted.getContext(), contextDeleted.preserveEventStore());
    }

    @Override
    public void stop() {
        stop(() -> {
        });
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void initContext(String context, boolean validating) {
        logger.debug("Init context: {}", context);
        localEventStore.initContext(context, validating);
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        runnable.run();
        running = false;
        runnable.run();
    }

    @Override
    public int getPhase() {
        return 40;
    }

    @Override
    public EventStore getEventStore(String context) {
        if (isLeader(context)) {
            return localEventStore;
        }
        String leader = leaderProvider.apply(context, true);
        if (leader == null) {
            return null;
        }
        return new RemoteEventStore(clusterNodeSupplier.apply(leader), messagingPlatformConfiguration, channelProvider);
    }

    @Override
    public EventStore getEventStore(String context, boolean useLocal) {
        if (useLocal && contextController.containsStorageContext(context)) {
            return localEventStore;
        }
        return getEventStore(context);
    }

    private boolean isLeader(String context) {
        return isLeader(nodeName, context, true);
    }

    @Override
    public boolean isLeader(String nodeName, String context, boolean wait) {
        String leader = leaderProvider.apply(context, wait);
        return leader != null && leader.equals(nodeName);
    }
}
