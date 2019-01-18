package io.axoniq.axonserver.enterprise.cluster.manager;

import com.google.common.hash.Hashing;
import io.axoniq.axonserver.LifecycleController;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.ManagedChannelHelper;
import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterServiceInterface;
import io.axoniq.axonserver.enterprise.cluster.internal.StubFactory;
import io.axoniq.axonserver.enterprise.cluster.internal.SyncStatusController;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.messaging.event.RemoteEventStore;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.StreamSupport.stream;

/**
 * Author: marc
 */
public class EventStoreManager implements SmartLifecycle, EventStoreLocator {
    private final Logger logger = LoggerFactory.getLogger(EventStoreManager.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final StubFactory stubFactory;
    private final LifecycleController lifecycleController;
    private final LocalEventStore localEventStore;
    private final SyncStatusController syncStatusController;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final Map<String,String> masterPerContext = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory("storage-manager-selector"));
    private volatile boolean running;
    private final Map<String,ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();

    private final Iterable<Context> dynamicContexts;
    private final boolean needsValidation;
    private final String nodeName;
    private final boolean clustered;
    private final long connectWaitTime;
    private final Function<String, ClusterNode> clusterNodeSupplier;

    public EventStoreManager(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             StubFactory stubFactory, LifecycleController lifecycleController,
                             LocalEventStore localEventStore,
                             SyncStatusController syncStatusController,
                             ApplicationEventPublisher applicationEventPublisher,
                             Iterable<Context> dynamicContexts, boolean needsValidation, String nodeName,
                             boolean clustered,
                             long connectWaitTime,
                             Function<String, ClusterNode> clusterNodeSupplier) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.stubFactory = stubFactory;
        this.lifecycleController = lifecycleController;
        this.localEventStore = localEventStore;
        this.syncStatusController = syncStatusController;
        this.applicationEventPublisher = applicationEventPublisher;
        this.dynamicContexts = dynamicContexts;
        this.needsValidation = needsValidation;
        this.nodeName = nodeName;
        this.clustered = clustered;
        this.connectWaitTime = connectWaitTime;
        this.clusterNodeSupplier = clusterNodeSupplier;
    }

    @Autowired
    public EventStoreManager(ContextController contextController,
                             MessagingPlatformConfiguration messagingPlatformConfiguration,
                             StubFactory stubFactory,
                             ClusterController clusterController,
                             LifecycleController lifecycleController,
                             LocalEventStore localEventStore,
                             SyncStatusController syncStatusController,
                             ApplicationEventPublisher applicationEventPublisher) {
        this(messagingPlatformConfiguration, stubFactory, lifecycleController, localEventStore, syncStatusController, applicationEventPublisher,
             () -> contextController.getContexts().iterator(), lifecycleController.isCleanShutdown(), clusterController.getName(), clusterController.isClustered(),
             messagingPlatformConfiguration.getCluster().getConnectionWaitTime(), clusterController::getNode);
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

    public void shutdown() {
        scheduledExecutorService.shutdown();
    }

    @EventListener
    public void on(ClusterEvents.MasterConfirmation masterConfirmation) {
        masterPerContext.put(masterConfirmation.getContext(),
                             masterConfirmation.getNode());
    }


    @EventListener
    public void on(ClusterEvents.MasterStepDown masterStepDown) {
        logger.info("{}: Master stepped down", masterStepDown.getContextName());
        masterPerContext.remove(masterStepDown.getContextName());
        if( !masterStepDown.isForwarded()) {
            localEventStore.cancel(masterStepDown.getContextName());
        }
        rescheduleElection(masterStepDown.getContextName());
    }

    @EventListener
    public void on(ClusterEvents.MasterDisconnected masterDisconnected) {
        logger.info("{}: Master {} disconnected", masterDisconnected.getContextName(),masterDisconnected.oldMaster());
        masterPerContext.remove(masterDisconnected.getContextName());
        rescheduleElection(masterDisconnected.getContextName());
    }

    private void rescheduleElection(String contextName) {
        ScheduledFuture<?> task = tasks.get(contextName);
        if (task == null || task.isDone()) {
            tasks.put(contextName, scheduledExecutorService.schedule(() -> startLeaderElection(
                    contextName), (long) (Math.random() * 1000), TimeUnit.MILLISECONDS));
        }
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected event) {
        masterPerContext.forEach((context, node) -> {
            if( node.equals(nodeName)) {
                event.getRemoteConnection().publish(ConnectorCommand.newBuilder().setMasterConfirmation(NodeContextInfo.newBuilder().setContext(context).setNodeName(node)).build());
            }
        });
    }

    @EventListener
    public void on(ClusterEvents.AxonServerInstanceDisconnected disconnected) {
        Set<String> contexts = new HashSet<>();
        masterPerContext.forEach((context, node) -> {
            if( node.equals(disconnected.getNodeName())) {
                contexts.add(context);
            }
        });

        contexts.forEach(c -> on(new ClusterEvents.MasterStepDown(c, true)));

    }

    @EventListener
    public void on(ContextEvents.ContextCreated contextCreated) {
        Context context = context(contextCreated.getName());
        initContext(context, false);
    }

    @EventListener
    public void on(ContextEvents.NodeRolesUpdated contextUpdated) {
        logger.debug("{}: updated {} storage: {}", contextUpdated.getName(), contextUpdated.getNode().getName(), contextUpdated.getNode().isStorage());
        try {
            Context context = context(contextUpdated.getName());
            logger.debug("{}: storage members {}", context.getName(), context.getStorageNodeNames());
            if (context.isStorageMember(nodeName)) {
                initContext(context, false);
            } else {
                localEventStore.cleanupContext(context.getName());
                if (isMaster(context.getName())) {
                    masterPerContext.remove(context.getName());
                    applicationEventPublisher.publishEvent(new ClusterEvents.MasterStepDown(context.getName(),
                                                                                            false));
                }
            }
        } catch( RuntimeException re) {
            logger.warn("Failed to process event {}", contextUpdated, re);
        }
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        localEventStore.cleanupContext(contextDeleted.getName());
        if( isMaster(contextDeleted.getName())) {
            masterPerContext.remove(contextDeleted.getName());
            applicationEventPublisher.publishEvent(new ClusterEvents.MasterStepDown(contextDeleted.getName(), false));
        }
    }

    @Override
    public void stop() {
        stop(() -> {});

    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void initContext(Context context, boolean validating) {
        if (!context.isStorageMember(nodeName)) return;
        logger.debug("Init context: {}", context.getName());
        localEventStore.initContext(context.getName(), validating);
        if (!clustered || context.getStorageNodes().size() == 1) {
            masterPerContext.put(context.getName(), nodeName);
            return;
        }

        if( masterPerContext.containsKey(context.getName())) {
            applicationEventPublisher.publishEvent(new ClusterEvents.MasterConfirmation(context.getName(),
                                                                                        masterPerContext.get(context.getName()), true));
        } else {
            logger.debug("Scheduling initial leader election");
            rescheduleElection(context.getName());
        }
    }

    public static int hash(String context, String node){
        return Hashing.goodFastHash(32).hashString(context + node,
                                                          Charset.defaultCharset()).asInt();

    }

    private void startLeaderElection(String contextName) {
        logger.debug("Start leader election for {}: master: {}", contextName, masterPerContext.get(contextName));
        if( ! running || masterPerContext.containsKey(contextName)) return;
        Context context = context(contextName);
        if( context == null || ! context.isStorageMember(nodeName)) return;

        NodeContextInfo nodeContextInfo = NodeContextInfo.newBuilder()
                                                         .setActiveSince(0)
                                                         .setContext(context.getName())
                                                         .setNodeName(nodeName)
                                                         .setMasterSequenceNumber(localEventStore.getLastToken(context.getName()))
                                                         .setHashKey(hash(contextName, nodeName))
                                                         .setNrOfMasterContexts(getNrOrMasterContexts(nodeName))
                                                         .setEventSafePoint(syncStatusController.getSafePoint(EventType.EVENT, contextName))
                                                         .setPrevMasterGeneration(syncStatusController.generation(EventType.EVENT, contextName))
                                                         .build();

        Set<ClusterNode> storageNodes = context.getStorageNodes();
        if( ! storageNodes.isEmpty() ) {
            CountDownLatch countdownLatch = new CountDownLatch(storageNodes.size() - 1);
            AtomicInteger responseCount = new AtomicInteger(1);
            AtomicBoolean approved = new AtomicBoolean(true);
            storageNodes
                    .stream()
                    .filter(node -> !node.getName().equals(nodeName))
                    .forEach(node -> requestBecomeLeader(node,
                                                         nodeContextInfo,
                                                         countdownLatch,
                                                         responseCount,
                                                         approved));

            try {
                if (!countdownLatch.await(connectWaitTime, TimeUnit.MILLISECONDS)) {
                    logger.debug("Timeout while waiting for responses from nodes");
                }
                if (!running || masterPerContext.containsKey(context.getName())) return;

                if (approved.get() && hasQuorumToChange(storageNodes.size(), responseCount.get())) {
                    logger.info("Become master");
                    masterPerContext.put(context.getName(), nodeName);
                    syncStatusController.increaseGenerations(contextName);
                    applicationEventPublisher.publishEvent(new ClusterEvents.BecomeMaster(context.getName(),
                                                                                          nodeName,
                                                                                          false));
                } else {
                    logger.debug("Rescheduling as no master found");
                    rescheduleElection(contextName);
                }
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for responses", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("Rescheduling on exception during leader election", e);
                rescheduleElection(contextName);
            }
        }
    }

    public int getNrOrMasterContexts(String name) {
        return masterPerContext.values().stream().mapToInt(master -> master.equals(name) ? 1 : 0).sum();
    }

    private boolean hasQuorumToChange(int nodesInCluster, int responseCount) {
        return responseCount >= nodesInCluster/2f + 0.01;
    }

    private void requestBecomeLeader(ClusterNode node, NodeContextInfo nodeContextInfo,
                                     CountDownLatch countdownLatch, AtomicInteger responseCount,
                                     AtomicBoolean approved) {
        logger.debug("Request become leader: {}", node.getName());
        MessagingClusterServiceInterface stub = stubFactory.messagingClusterServiceStub(
                node);
        stub.requestLeader(nodeContextInfo,
                           new StreamObserver<Confirmation>() {
                               @Override
                               public void onNext(Confirmation confirmation) {
                                   if (!confirmation.getSuccess()) {
                                       approved.set(false);
                                   }
                                   responseCount.incrementAndGet();
                                   countdownLatch.countDown();
                               }

                               @Override
                               public void onError(Throwable throwable) {
                                   countdownLatch.countDown();
                                   ManagedChannelHelper.checkShutdownNeeded(node.getName(), throwable);
                                   if( ! masterPerContext.containsKey(nodeContextInfo.getContext()) ) {
                                       logger.warn("Error while requesting to become leader for {}: {}",
                                                   node.getName(),
                                                   throwable.getMessage());
                                       if (logger.isDebugEnabled()) {
                                           logger.debug("Stacktrace", throwable);
                                       }
                                   }
                               }

                               @Override
                               public void onCompleted() {
                                    // don't need to wait for completed, response already processed in the onNext.
                               }
                           });
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        masterFor().forEach(c -> on(new ClusterEvents.MasterStepDown(c, false)));
        runnable.run();
        running = false;
    }

    @Override
    public int getPhase() {
        return 40;
    }

    public Stream<String> masterFor() {
        return contextsStream().filter(context -> nodeName.equals(masterPerContext.get(context.getName()))).map(Context::getName);
    }

    public EventStore getEventStore(String context) {
        logger.debug("Get master for: {}  = {} local {}", context, masterPerContext.get(context),
                     nodeName.equals(masterPerContext.get(context)));
        if( isMaster( context)) {
            return localEventStore;
        }
        String master = masterPerContext.get(context);
        if( master == null) return null;
        return new RemoteEventStore(clusterNodeSupplier.apply(master), messagingPlatformConfiguration);
    }

    public String getMaster(String context) {
        return masterPerContext.get(context);
    }

    public boolean isMaster(String context) {
        return isMaster(nodeName, context);
    }
    public boolean isMaster(String nodeName, String context) {
        String master = masterPerContext.get(context);
        return master != null && master.equals(nodeName);
    }

    private Stream<Context> contextsStream() {
        return stream(dynamicContexts.spliterator(), false);
    }

    private Context context(String context) {
        return contextsStream().filter(c -> context.equals(c.getName())).findFirst()
                               .orElseThrow(() -> new IllegalArgumentException(context + " context doesn't exist"));
    }
}
