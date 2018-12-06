package io.axoniq.axonserver.enterprise.cluster.manager;

import com.google.common.hash.Hashing;
import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.messaging.event.RemoteEventStore;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.topology.EventStoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;

import java.nio.charset.Charset;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Author: marc
 */
public class EventStoreManager implements SmartLifecycle, EventStoreLocator {
    private final Logger logger = LoggerFactory.getLogger(EventStoreManager.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final LifecycleController lifecycleController;
    private final LocalEventStore localEventStore;
    private volatile boolean running;

    private final Iterable<String> dynamicContexts;
    private final Function<String, String> masterProvider;
    private final boolean needsValidation;
    private final String nodeName;
    private final Function<String, ClusterNode> clusterNodeSupplier;

    public EventStoreManager(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             LifecycleController lifecycleController,
                             LocalEventStore localEventStore,
                             Iterable<String> dynamicContexts,
                             Function<String, String> masterProvider,
                             boolean needsValidation, String nodeName,
                             Function<String, ClusterNode> clusterNodeSupplier) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.lifecycleController = lifecycleController;
        this.localEventStore = localEventStore;
        this.dynamicContexts = dynamicContexts;
        this.masterProvider = masterProvider;
        this.needsValidation = needsValidation;
        this.nodeName = nodeName;
        this.clusterNodeSupplier = clusterNodeSupplier;
    }

    @Autowired
    public EventStoreManager(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             ClusterController clusterController,
                             LifecycleController lifecycleController,
                             GrpcRaftController raftController,
                             LocalEventStore localEventStore) {
        this(messagingPlatformConfiguration, lifecycleController, localEventStore,
             () -> raftController.getContexts().iterator(),
             raftController::getStorageMaster,
             lifecycleController.isCleanShutdown(), messagingPlatformConfiguration.getName(), clusterController::getNode);
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


//    @EventListener
//    public void on(ContextEvents.ContextCreated contextCreated) {
//        initContext(contextCreated.getName(), false);
//    }
//
//    @EventListener
//    public void on(ContextEvents.ContextDeleted contextDeleted) {
//        localEventStore.cleanupContext(contextDeleted.getName());
//    }
//
    @Override
    public void stop() {
        stop(() -> {});

    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void initContext(String context, boolean validating) {
        logger.debug("Init context: {}", context);
        localEventStore.initContext(context, validating);
    }

    public static int hash(String context, String node){
        return Hashing.goodFastHash(32).hashString(context + node,
                                                          Charset.defaultCharset()).asInt();

    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        runnable.run();
        running = false;
    }

    @Override
    public int getPhase() {
        return 40;
    }

    public Stream<String> masterFor() {
        return Stream.empty();
    }

    public EventStore getEventStore(String context) {
        if( isMaster( context)) {
            return localEventStore;
        }
        String master = getMaster(context);
        if( master == null) return null;
        return new RemoteEventStore(clusterNodeSupplier.apply(master), messagingPlatformConfiguration);
    }

    public String getMaster(String context) {
        return masterProvider.apply(context);
    }

    public boolean isMaster(String context) {
        return isMaster(nodeName, context);
    }

    public boolean isMaster(String nodeName, String context) {
        String master = masterProvider.apply(context);
        return master != null && master.equals(nodeName);
    }


}
