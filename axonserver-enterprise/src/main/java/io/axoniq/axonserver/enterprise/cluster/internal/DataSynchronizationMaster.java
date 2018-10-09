package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.storage.transaction.ReplicationManager;
import io.axoniq.axonserver.grpc.ReceivingStreamObserver;
import io.axoniq.axonserver.grpc.SendingStreamObserver;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.internal.DataSynchronizerGrpc;
import io.axoniq.axonserver.grpc.internal.SafepointMessage;
import io.axoniq.axonserver.grpc.internal.StartSynchronization;
import io.axoniq.axonserver.grpc.internal.SynchronizationReplicaInbound;
import io.axoniq.axonserver.grpc.internal.SynchronizationReplicaOutbound;
import io.axoniq.axonserver.grpc.internal.TransactionConfirmation;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Author: marc
 */
@Controller
public class DataSynchronizationMaster extends DataSynchronizerGrpc.DataSynchronizerImplBase implements
        ReplicationManager, ApplicationContextAware {
    private final Logger logger = LoggerFactory.getLogger(DataSynchronizationMaster.class);
    private final Map<String, Integer> quorumPerContext = new ConcurrentHashMap<>();
    private final Map<EventTypeContext, Consumer<Long>> confirmationListeners = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Replica>> connectionsPerContext = new ConcurrentHashMap<>();
    private final ContextController contextController;
    private final ApplicationEventPublisher eventPublisher;
    private ApplicationContext applicationContext;

    public DataSynchronizationMaster(ContextController contextController,
                                     ApplicationEventPublisher eventPublisher) {
        this.contextController = contextController;
        this.eventPublisher = eventPublisher;
    }


    @Override
    public int getQuorum(String context) {
        return quorumPerContext.computeIfAbsent(context, c -> (int) Math.ceil((contextController.getContext(c).getStorageNodes().size() + 0.1)/2));
    }

    @Override
    public void registerListener(EventTypeContext type, Consumer<Long> replicationCompleted) {
        confirmationListeners.put(type, replicationCompleted);
    }

    @Override
    public void publish(EventTypeContext type, List<Event> eventList, long token) {
        TransactionWithToken transactionWithToken = TransactionWithToken.newBuilder()
                                                                        .setToken(token)
                                                                        .addAllEvents(eventList)
                                                                        .build();
        Map<String, Replica> connections = connectionsPerContext.get(type.getContext());
        if( connections == null) {
            logger.trace("No connections found for context {}, cannot synchronize message", type.getContext());
            return;
        }
        SynchronizationReplicaInbound message ;
        if (type.getEventType() == EventType.SNAPSHOT) {
            message = SynchronizationReplicaInbound.newBuilder().setSnapshot(transactionWithToken).build();
            connections.forEach((name,r) -> r.publishSnapshot(message));
        } else if (type.getEventType() == EventType.EVENT) {
            message = SynchronizationReplicaInbound.newBuilder().setEvent(transactionWithToken).build();
            connections.forEach((name,r) -> r.publishEvent(message));
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public void publishSafepoints(String context, long eventToken, long snapshotToken) {
        SynchronizationReplicaInbound eventSynchronizationMessage =
                safepointMessage(context, eventToken, EventType.EVENT);
        SynchronizationReplicaInbound snapshotSynchronizationMessage =
                safepointMessage(context, snapshotToken, EventType.SNAPSHOT);

        Map<String, Replica> targets = connectionsPerContext.get(context);
        if( targets != null) {
            targets.forEach((n,replica) -> replica.sendMessage(eventSynchronizationMessage));
            targets.forEach((n,replica) -> replica.sendMessage(snapshotSynchronizationMessage));
        }
    }

    @EventListener
    public void on(ClusterEvents.MasterStepDown masterStepDown) {
        Map<String,Replica> replicas = connectionsPerContext.remove(masterStepDown.getContextName());
        if( replicas != null) {
            replicas.forEach((n,replica) -> replica.disconnect());
        }
    }

    @EventListener
    public void on(ContextEvents.ContextDeleted contextDeleted) {
        Map<String,Replica> replicas = connectionsPerContext.remove(contextDeleted.getName());
        if( replicas != null) {
            replicas.forEach((n,replica) -> replica.disconnect());
        }
    }

    @EventListener
    public void on(ContextEvents.NodeRolesUpdated nodeRolesUpdated) {
        if( !nodeRolesUpdated.getNode().isStorage()) {
            Map<String,Replica> replicas = connectionsPerContext.get(nodeRolesUpdated.getName());
            if( replicas != null) {
                Replica replica = replicas.remove(nodeRolesUpdated.getNode().getName());
                if (replica != null) {
                    replica.disconnect();
                }
            }
        }
        quorumPerContext.remove(nodeRolesUpdated.getName());
    }

    private SynchronizationReplicaInbound safepointMessage(String context, long eventToken, EventType eventType) {
        return SynchronizationReplicaInbound.newBuilder()
                                            .setSafepoint(
                                                    SafepointMessage
                                                            .newBuilder()
                                                            .setContext( context)
                                                            .setType(eventType.name())
                                                            .setToken(eventToken)
                                                            .build())
                                            .build();
    }

    class Replica {

        private final String nodeName;
        private final String context;
        private final AtomicLong lastEventTransaction = new AtomicLong();
        private final AtomicLong lastSnapshotTransaction = new AtomicLong();
        private final AtomicBoolean streamingEvents = new AtomicBoolean();
        private final AtomicBoolean streamingSnapshots = new AtomicBoolean();

        private final StreamObserver<SynchronizationReplicaInbound> replicaInboundStreamObserver;
        private final AtomicLong permits = new AtomicLong();

        private Replica(String nodeName, String context,
                       StreamObserver<SynchronizationReplicaInbound> replicaInboundStreamObserver) {
            this.nodeName = nodeName;
            this.context = context;
            this.replicaInboundStreamObserver = replicaInboundStreamObserver;
        }

        private boolean publish(SynchronizationReplicaInbound message) {
            if( permits.decrementAndGet() > 0) {
                try {
                    replicaInboundStreamObserver.onNext(message);
                } catch( RuntimeException runtimeException) {
                    logger.warn("{}: Send message to {} failed", context, nodeName, runtimeException);
                    cancel(this);
                    return false;
                }
                return true;
            }
            logger.trace("No permits left: {}", permits);
            return false;
        }

        public void init(long eventToken, long snapshotToken, long permits) {
            this.permits.set(permits);
            startStreaming(eventToken, snapshotToken);
        }

        private void startStreaming(long eventToken, long snapshotToken) {
            LocalEventStore localEventStore = applicationContext.getBean(LocalEventStore.class);
            streamingEvents.set(true);
            streamingSnapshots.set(true);
            localEventStore.streamEventTransactions(context, eventToken, this::publishEventFromStream)
                           .exceptionally(this::streamingFailed)
                           .thenAccept(v -> streamingEvents.set(false));
            localEventStore.streamSnapshotTransactions(context, snapshotToken, this::publishSnapshotFromStream)
                           .exceptionally(this::streamingFailed)
                           .thenAccept(v -> streamingSnapshots.set(false));
        }

        private Void streamingFailed(Throwable cause) {
            replicaInboundStreamObserver.onError(cause);
            return null;
        }

        private boolean publishEventFromStream(TransactionWithToken transactionWithToken) {
            if( transactionWithToken.getToken() < lastEventTransaction.get()) {
                return true;
            }
            SynchronizationReplicaInbound message = SynchronizationReplicaInbound.newBuilder().setEvent(transactionWithToken).build();
            return publish(message);
        }
        private boolean publishSnapshotFromStream(TransactionWithToken transactionWithToken) {
            if( transactionWithToken.getToken() < lastSnapshotTransaction.get()) return true;
            SynchronizationReplicaInbound message = SynchronizationReplicaInbound.newBuilder().setSnapshot(transactionWithToken).build();
            return publish(message);
        }

        public void publishEvent(SynchronizationReplicaInbound message) {
            if( streamingEvents.get() && message.getEvent().getToken() > lastEventTransaction.get() + 1000) {
                if( logger.isTraceEnabled() ) logger.trace("Not sending event {} as lastConformed ({}) too far behind", message.getEvent().getToken(),
                                                           lastEventTransaction.get());
                return;
            }
            publish(message);
        }
        public void publishSnapshot(SynchronizationReplicaInbound message) {
            if( streamingSnapshots.get() && message.getSnapshot().getToken() > lastSnapshotTransaction.get() + 1000) {
                if( logger.isTraceEnabled() ) logger.trace("Not sending snapshot {} as lastConformed ({}) too far behind", message.getEvent().getToken(),
                                                           lastSnapshotTransaction.get());
                return;
            }
            publish(message);
        }

        public String getNodeName() {
            return nodeName;
        }

        public long getLastEventTransaction() {
            return lastEventTransaction.get();
        }

        public long getLastSnapshotTransaction() {
            return lastSnapshotTransaction.get();
        }

        public void addPermits(long newPermits) {
            logger.info("Received new permits {}, {} left", newPermits, permits);
            long before = permits.getAndAccumulate(newPermits, (old, inc) -> Math.max(old, 0) + inc);
            if( before <= 0) {
                logger.debug("restart streaming with {} permits", newPermits);
                startStreaming(lastEventTransaction.get(), lastSnapshotTransaction.get());
            }
        }

        public void sendMessage(SynchronizationReplicaInbound synchronizationReplicaInbound) {
            try {
                replicaInboundStreamObserver.onNext(synchronizationReplicaInbound);
            } catch (StatusRuntimeException sre) {
                logger.warn("{}: Send message to {} failed", context, nodeName, sre);
                cancel(this);
            }
        }

        public void disconnect() {
            replicaInboundStreamObserver.onCompleted();
        }

    }


    @Override
    public StreamObserver<SynchronizationReplicaOutbound> openConnection(
            StreamObserver<SynchronizationReplicaInbound> responseObserverOrg) {
        SendingStreamObserver<SynchronizationReplicaInbound> replicaInboundStreamObserver = new SendingStreamObserver<>(responseObserverOrg);
        return new ReceivingStreamObserver<SynchronizationReplicaOutbound>(logger) {
            private volatile Replica replica;
            private volatile String context;

            @Override
            protected void consume(SynchronizationReplicaOutbound synchronizationReplicaOutbound) {
                switch (synchronizationReplicaOutbound.getRequestCase()) {
                    case START:
                        StartSynchronization start = synchronizationReplicaOutbound
                                .getStart();
                        context = start.getContext();
                        replica = new Replica(start.getNodeName(), context, replicaInboundStreamObserver);
                        replica.lastEventTransaction.set(start.getEventToken()-1);
                        replica.lastSnapshotTransaction.set(start.getSnaphshotToken()-1);
                        connectionsPerContext.computeIfAbsent(start.getContext(), c -> new ConcurrentHashMap<>())
                                             .put(replica.nodeName, replica);

                        replica.init(start.getEventToken(), start.getSnaphshotToken(), start.getPermits());
                        break;
                    case PERMITS:
                        replica.addPermits(synchronizationReplicaOutbound.getPermits().getPermits());
                        break;
                    case CONFIRMATION:
                        TransactionConfirmation confirmation = synchronizationReplicaOutbound
                                .getConfirmation();
                        EventType i = EventType.valueOf(confirmation.getType());
                        if (i == EventType.SNAPSHOT) {
                            EventTypeContext snapshotTypeContext = new EventTypeContext(context, EventType.SNAPSHOT);
                            confirmationListeners.get(snapshotTypeContext).accept(confirmation.getToken());
                            replica.lastSnapshotTransaction.set(confirmation.getToken());
                        } else if (i == EventType.EVENT) {
                            EventTypeContext eventTypeContext = new EventTypeContext(context, EventType.EVENT);
                            confirmationListeners.get(eventTypeContext).accept(confirmation.getToken());
                            replica.lastEventTransaction.set(confirmation.getToken());
                        }
                        break;
                    case SAFEPOINT_CONFIRMATION:
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }

            }

            @Override
            protected String sender() {
                return replica != null ? replica.nodeName : "None";
            }

            @Override
            public void onError(Throwable throwable) {
                if( replica != null) {
                    logger.warn("{}: Error on connection from {}", context, replica.nodeName, throwable);
                    cancel(replica);
                }
            }

            @Override
            public void onCompleted() {
                if( replica != null) {
                    replica.replicaInboundStreamObserver.onCompleted();
                    cancel(replica);
                }
            }
        };
    }

    private void cancel(Replica replica) {
        Map<String, Replica> connection = connectionsPerContext.get(replica.context);
        if( connection != null) {
            connection.remove(replica.nodeName);
            checkQuorum(replica.context);
        }
    }

    private void checkQuorum(String contextName) {
        Context context = contextController.getContext(contextName);
        int replicas = connectionsPerContext.get(contextName).size();
        int total = context.getStorageNodes().size();
        if( replicas + 1 < Math.ceil(total/2f) ) {
            eventPublisher.publishEvent(new ClusterEvents.MasterStepDown(contextName, false));
        }
    }

    public Map<String, Map<String,Replica>> getConnectionsPerContext() {
        return connectionsPerContext;
    }
}
