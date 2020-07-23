package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * Component to start and stop event stores for a specific context. Initializes the event stores on startup of the node.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@Component
public class EventStoreManager implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(EventStoreManager.class);
    private final LifecycleController lifecycleController;
    private final LocalEventStore localEventStore;
    private volatile boolean running;

    private final boolean needsValidation;
    private final RaftGroupRepositoryManager raftGroupRepositoryManager;


    /**
     * Constructor for the {@link EventStoreManager}.
     *
     * @param lifecycleController        checks if application had clean shutdown before restart
     * @param raftGroupRepositoryManager provides context information
     * @param localEventStore            provides access to local copy of the event store
     */
    public EventStoreManager(LifecycleController lifecycleController,
                             RaftGroupRepositoryManager raftGroupRepositoryManager,
                             LocalEventStore localEventStore) {
        this.needsValidation = !lifecycleController.isCleanShutdown();
        this.lifecycleController = lifecycleController;
        this.raftGroupRepositoryManager = raftGroupRepositoryManager;
        this.localEventStore = localEventStore;
    }


    /**
     * Starts all contexts on the current node. If application did not have a clean shutdown, it will perform
     * a validation of the data in the last segments.
     */
    public void start() {
        try {
            raftGroupRepositoryManager.storageContexts().forEach(context -> initContext(context,
                                                                                        needsValidation,
                                                                                        0L,
                                                                                        0L));
        } catch (RuntimeException t) {
            logger.error("Failed to start storage: {}", t.getMessage(), t);
            lifecycleController.abort();
        }
        lifecycleController.setCleanShutdown();
        running = true;
    }

    /**
     * Notifies the local event store that this node is no longer leader for a specific context.
     *
     * @param leaderStepDown event containing the context name
     */
    @EventListener
    public void on(ClusterEvents.ContextLeaderStepDown leaderStepDown) {
        localEventStore.cancel(leaderStepDown.context());
    }

    /**
     * Received notification that the current node has become leader for a context. Ensures that the event
     * store for this context is initialized.
     *
     * @param becomeLeader event containing the context name
     */
    @EventListener
    public void on(ClusterEvents.BecomeContextLeader becomeLeader) {
        if (!isAdmin(becomeLeader.context())) {
            initContext(becomeLeader.context(), false, 0L, 0L);
        }
    }

    /**
     * Received notification that a context has been created. Initializes the event store for that context.
     *
     * @param contextCreated event containing the name of the created context
     */
    @EventListener
    public void on(ContextEvents.ContextCreated contextCreated) {
        if (!RoleUtils.hasStorage(contextCreated.role())) {
            return;
        }
        initContext(contextCreated.context(),
                    false,
                    contextCreated.defaultFirstEventToken(),
                    contextCreated.defaultFirstSnapshotToken());
    }

    /**
     * Received notification that a context has been deleted. Stops the event store for that context and optionally
     * deletes the data in the event store.
     *
     * @param contextDeleted event containing the name of the deleted context
     */
    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        logger.info("{}: close context", contextDeleted.context());
        localEventStore.deleteContext(contextDeleted.context(), contextDeleted.preserveEventStore());
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void initContext(String context, boolean validating, long defaultFirstEventToken,
                             long defaultFirstSnapshotToken) {
        if (!isAdmin(context)) {
            logger.debug("Init context: {}", context);
            localEventStore.initContext(context, validating, defaultFirstEventToken, defaultFirstSnapshotToken);
        }
    }

    @Override
    public int getPhase() {
        return 40;
    }
}
