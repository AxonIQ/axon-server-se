package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.exception.StreamAlreadyClosedException;
import io.axoniq.axonserver.cluster.replication.DefaultSnapshotContext;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.cluster.util.MaxMessageSizePredicate;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Peer responsible for replication: appending entries, installing snapshot and sending heartbeats.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class ReplicatorPeer {

    private interface ReplicatorPeerState {

        default void start() {
        }

        default void stop() {
        }

        int sendNextEntries();
    }

    private class IdleReplicatorPeerState implements ReplicatorPeerState {

        @Override
        public int sendNextEntries() {
            return 0;
        }

        @Override
        public String toString() {
            return "Idle Replicator Peer State";
        }
    }

    private class InstallSnapshotState implements ReplicatorPeerState {

        private static final int RESERVED_FOR_OTHER_FIELDS = 10000;

        private final int grpcConfiguredMaxMessageSize = raftGroup.raftConfiguration().maxMessageSize();
        private  final int snapshotChunksBufferSize = raftGroup.raftConfiguration().maxSnapshotNoOfChunksPerBatch();
        private final int maxMessageSize;

        private final SnapshotContext snapshotInstallationContext;
        private Registration registration;
        private Subscription subscription;
        private AtomicInteger offset = new AtomicInteger();
        private volatile int lastReceivedOffset;
        private volatile boolean done = false;
        private volatile long lastAppliedIndex;

        public InstallSnapshotState(
                SnapshotContext snapshotInstallationContext) {
            this.snapshotInstallationContext = snapshotInstallationContext;
            this.maxMessageSize = grpcConfiguredMaxMessageSize - RESERVED_FOR_OTHER_FIELDS;
        }

        @Override
        public void start() {
            offset.set(0);
            logger.info("{} in term {}: start snapshot installation: {}",
                        groupId(),
                        currentTerm(),
                        snapshotInstallationContext);
            registration = raftPeer.registerInstallSnapshotResponseListener(this::handleResponse);
            lastAppliedIndex = lastAppliedIndex();
            long lastIncludedTerm = lastAppliedTerm();
            MaxMessageSizePredicate maxMessageSizePredicate = new MaxMessageSizePredicate(maxMessageSize/10,snapshotChunksBufferSize);
            snapshotManager.streamSnapshotData(snapshotInstallationContext)
                           //Buffer serializedObjects until the max grpc message & chunk size is met
                           .bufferUntil(p -> maxMessageSizePredicate.test(p.getSerializedSize()), true)
                           .subscribe(new Subscriber<List<SerializedObject>>() {
                               @Override
                               public void onSubscribe(Subscription s) {
                                   subscription = s;
                               }

                               @Override
                               public void onNext(List<SerializedObject> serializedObjects) {
                                   int chunk = offset.getAndIncrement();
                                   InstallSnapshotRequest.Builder requestBuilder =
                                           InstallSnapshotRequest.newBuilder()
                                                                 .setRequestId(UUID.randomUUID().toString())
                                                                 .setGroupId(groupId())
                                                                 .setTerm(currentTerm())
                                                                 .setLeaderId(me())
                                                                 .setLastIncludedTerm(lastIncludedTerm)
                                                                 .setLastIncludedIndex(lastAppliedIndex)
                                                                 .setOffset(chunk)
                                                                 .setDone(done)
                                                                 .addAllData(serializedObjects);
                                   logger.trace("{} in term {}: Sending install snapshot chunk with offset: {}",
                                                groupId(),
                                                currentTerm(),
                                                chunk);
                                   if (firstChunk(chunk)) {
                                       requestBuilder.setLastConfig(raftGroup.raftConfiguration().config());
                                   }
                                   send(requestBuilder.build());
                               }

                               @Override
                               public void onError(Throwable t) {
                                   logger.error("{} in term {}: Install snapshot failed.", groupId(), currentTerm(), t);
                                   changeStateTo(new AppendEntryState());
                               }

                               @Override
                               public void onComplete() {
                                   done = true;
                                   int chunk = offset.getAndIncrement();
                                   InstallSnapshotRequest.Builder requestBuilder =
                                           InstallSnapshotRequest.newBuilder()
                                                                 .setRequestId(UUID.randomUUID().toString())
                                                                 .setGroupId(groupId())
                                                                 .setTerm(currentTerm())
                                                                 .setLeaderId(me())
                                                                 .setLastIncludedTerm(lastIncludedTerm)
                                                                 .setLastIncludedIndex(lastAppliedIndex)
                                                                 .setOffset(chunk)
                                                                 .setDone(done);
                                   if (firstChunk(chunk)) {
                                       requestBuilder.setLastConfig(raftGroup.raftConfiguration().config());
                                   }
                                   send(requestBuilder.build());

                                   logger.info("{} in term {}: Sending the last chunk for install snapshot to {}.",
                                               groupId(),
                                               currentTerm(),
                                               raftPeer.nodeId());
                               }
                           });
        }

        private boolean firstChunk(int chunk) {
            return chunk == 0;
        }

        @Override
        public void stop() {
            registration.cancel();
            if (subscription != null) {
                subscription.cancel();
                subscription = null;
            }
        }

        /**
         * Sends one InstallSnapshot request to the peer. A single request can contain a number of objects (limited by the transaction
         * size and the configuration parameter max-snapshot-chunks-per-batch).
         * @return 1 to force the replication thread to keep on attempting to send data without waiting
         */
        @Override
        public int sendNextEntries() {
            if (canSend()) {
                subscription.request(1);
            }
            return 1;
        }

        private void send(InstallSnapshotRequest request) {
            if( logger.isTraceEnabled()) {
                logger.trace("{} in term {}: Send request to {}: {}/{}.",
                            groupId(),
                            currentTerm(),
                            raftPeer.nodeId(),
                            request.getSerializedSize(), request.getDataCount());
            }
            raftPeer.installSnapshot(request);
            lastMessageSent.getAndUpdate(old -> Math.max(old, clock.millis()));
        }

        public void handleResponse(InstallSnapshotResponse response) {
            logger.trace("{} in term {}: Install snapshot - received response: {}.",
                         groupId(),
                         currentTerm(),
                         response);
            if (response.hasSuccess()) {
                lastMessageReceived.getAndUpdate(old -> Math.max(old, clock.millis()));
                lastReceivedOffset = response.getSuccess().getLastReceivedOffset();
                if (done) {
                    setMatchIndex(lastAppliedIndex);
                    logger.info("{} in term {}: Install snapshot confirmation received: {}, matchIndex {}, nextIndex {}",
                                groupId(),
                                currentTerm(),
                                response, matchIndex.get(), nextIndex.get());
                    changeStateTo(new AppendEntryState());
                }
            } else {
                logger.info("{} in term {}: Install snapshot message failed. Reason: {}.",
                            groupId(),
                            currentTerm(),
                            response.getFailure().getCause());
                if (currentTerm() < response.getTerm()) {
                    logger.info("{} in term {}: Install snapshot - Replica has higher term: {}",
                                groupId(),
                                currentTerm(),
                                response.getTerm());
                    String cause = format("%s in term %s: %s received InstallSnapshotResponse with term = %s from %s",
                                          groupId(),
                                          currentTerm(),
                                          me(),
                                          response.getTerm(),
                                          response.getResponseHeader().getNodeId());
                    updateCurrentTerm.accept(response.getTerm(), cause);
                }
                changeStateTo(new AppendEntryState());
            }
        }

        private boolean canSend() {
            return subscription != null &&
                    running &&
                    raftPeer.isReadyForSnapshot() &&
                    offset.get() - lastReceivedOffset < raftGroup.raftConfiguration().snapshotFlowBuffer();
        }

        @Override
        public String toString() {
            return "Install Snapshot Replicator Peer State";
        }
    }

    private class AppendEntryState implements ReplicatorPeerState {

        private volatile EntryIterator entryIterator;
        private Registration registration;
        private volatile boolean logCannotSend = true;

        @Override
        public void start() {
            registration = raftPeer.registerAppendEntriesResponseListener(this::handleResponse);
            logCannotSend = true;
            sendHeartbeat();
        }

        @Override
        public void stop() {
            Optional.ofNullable(registration).ifPresent(Registration::cancel);
        }

        /**
         * Sends next entries in the raft log to the peer. The number of messages is limited to the configured maxEntriesPerBatch,
         * and total time allowed to send messages is limited to the heartbeat timeout.
         * Method also stops sending messages when the StreamObserver (in raftPeer) is not ready (too many waiting bytes).
         *
         * @return number of entries sent
         */
        @Override
        public int sendNextEntries() {
            int sent = 0;
            try {
                long maxTime = System.currentTimeMillis() + raftGroup.raftConfiguration().heartbeatTimeout();
                EntryIterator iterator = entryIterator;
                if (iterator == null) {
                    nextIndex.compareAndSet(0, raftGroup.localLogEntryStore().lastLogIndex() + 1);
                    logger.debug("{} in term {}: create entry iterator for {} at {}",
                                 groupId(),
                                 currentTerm(),
                                 raftPeer.nodeId(),
                                 nextIndex);
                    iterator = updateEntryIterator();
                }

                if (iterator == null) {
                    return sent;
                }

                if (logCannotSend && !canSend()) {
                    logger.trace("{} in term {}: Trying to send to {} (nextIndex = {}, matchIndex = {}, lastLog = {})",
                                groupId(),
                                currentTerm(),
                                raftPeer.nodeId(),
                                nextIndex,
                                matchIndex,
                                raftGroup.localLogEntryStore().lastLogIndex());
                    logCannotSend = false;
                }
                while (canSend()
                        && System.currentTimeMillis() < maxTime
                        && sent < raftGroup.raftConfiguration().maxEntriesPerBatch() && iterator.hasNext()) {
                    Entry entry = iterator.next();
                    //
                    TermIndex previous = iterator.previous();
                    logger.trace("{} in term {}: Send request {} to {}: {}",
                                 groupId(),
                                 currentTerm(),
                                 sent,
                                 raftPeer.nodeId(),
                                 entry.getIndex());
                    send(AppendEntriesRequest.newBuilder()
                                             .setRequestId(UUID.randomUUID().toString())
                                             .setGroupId(groupId())
                                             .setPrevLogIndex(previous == null ? 0 : previous.getIndex())
                                             .setPrevLogTerm(previous == null ? 0 : previous.getTerm())
                                             .setTerm(currentTerm())
                                             .setLeaderId(me())
                                             .setTargetId(raftPeer.nodeId())
                                             .setCommitIndex(raftGroup.logEntryProcessor().commitIndex())
                                             .addEntries(entry)
                                             .build());
                    nextIndex.set(entry.getIndex() + 1);
                    sent++;
                }

                long now = clock.millis();
                if (sent == 0 && now - lastMessageSent.get() > raftGroup.raftConfiguration().heartbeatTimeout()) {
                    sendHeartbeat();
                }

                long after = System.currentTimeMillis();
                if( after - maxTime > raftGroup.raftConfiguration().heartbeatTimeout()) {
                    logger.info("{} in term {}: sending nextEntries to {} took {}ms",
                                groupId(),
                                currentTerm(),
                                raftPeer.nodeId(),
                                after-maxTime);
                }
            } catch (StreamAlreadyClosedException ex) {
                // Remote peer has sent failure and connection is closed, wait before sending more
                updateEntryIterator();
            } catch (RuntimeException ex) {
                logger.warn("{} in term {}: Sending nextEntries to {} failed.",
                            groupId(),
                            currentTerm(),
                            raftPeer.nodeId(),
                            ex);
                updateEntryIterator();
            }
            return sent;
        }

        public void handleResponse(AppendEntriesResponse response) {
            logger.trace("{} in term {}: Received response from {}: {}.",
                         groupId(),
                         currentTerm(),
                         raftPeer.nodeId(),
                         response);
            if (response.hasFailure()) {
                logger.info("{} in term {}: Sending append entry to {} failed. Reason: {}. Last applied index: {}, match index: {}.",
                            groupId(),
                            currentTerm(),
                            nodeId(),
                            response.getFailure().getCause(),
                            response.getFailure().getLastAppliedIndex(),
                            matchIndex());
                if (currentTerm() < response.getTerm()) {
                    logger.info("{} in term {}: Replica has higher term: {}.",
                                groupId(),
                                currentTerm(),
                                response.getTerm());
                    String cause = format("%s: %s received AppendEntriesResponse with term = %s from %s",
                                          groupId(),
                                          me(),
                                          response.getTerm(),
                                          response.getResponseHeader().getNodeId());
                    updateCurrentTerm.accept(response.getTerm(), cause);
                    return;
                }
                setMatchIndex(response.getFailure().getLastAppliedIndex());
                nextIndex.set(response.getFailure().getLastAppliedIndex() + 1);
                snapshotContext.set(new DefaultSnapshotContext(response.getFailure()));
                updateEntryIterator();
            } else {
                lastMessageReceived.getAndUpdate(old -> Math.max(old, clock.millis()));
                setMatchIndex(response.getSuccess().getLastLogIndex());
            }

            if (canSend()) {
                logCannotSend = true;
            }
        }

        public void sendHeartbeat() {
            logger.trace("{} in term {}: Sending heartbeat to {}.", groupId(), currentTerm(), nodeId());
            long commitIndex = raftGroup.logEntryProcessor().commitIndex();
            TermIndex lastTermIndex = raftGroup.localLogEntryStore().lastLog();
            AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                                                                 .setRequestId(UUID.randomUUID().toString())
                                                                 .setCommitIndex(commitIndex)
                                                                 .setLeaderId(me())
                                                                 .setGroupId(raftGroup.raftConfiguration().groupId())
                                                                 .setTerm(raftGroup.localElectionStore()
                                                                                   .currentTerm())
                                                                 .setTargetId(raftPeer.nodeId())
                                                                 .setPrevLogTerm(lastTermIndex.getTerm())
                                                                 .setPrevLogIndex(lastTermIndex.getIndex())
                                                                 .build();
            send(heartbeat);
        }

        private void send(AppendEntriesRequest request) {
            logger.trace("{}: Send request to {}: {}", groupId(), raftPeer.nodeId(), request);
            raftPeer.appendEntries(request);
            logger.trace("{}: Request sent to {}: {}", groupId(), raftPeer.nodeId(), request);
            lastMessageSent.getAndUpdate(old -> Math.max(old, clock.millis()));
        }

        private boolean forceSnapshot() {
            if( ! raftGroup.raftConfiguration().forceSnapshotOnJoin()) return false;
            return nextIndex() <= 1;
        }

        private EntryIterator updateEntryIterator() {
            LogEntryStore logEntryStore = raftGroup.localLogEntryStore();
            logger.info("{} in term {}: updateEntryIterator nextIndex = {}, matchIndex = {}",
                        groupId(),
                        currentTerm(),
                        nextIndex(),
                        matchIndex());

            if ( ! forceSnapshot() && (logEntryStore.firstLogIndex() <= 1 ||
                                        nextIndex() - 1 >= logEntryStore.firstLogIndex())) {
                entryIterator = logEntryStore.createIterator(nextIndex());
                return entryIterator;
            } else {
                logger.info("{} in term {}: follower {} is far behind the log entry. Follower's last applied index: {}.",
                            groupId(),
                            currentTerm(),
                            raftPeer.nodeId(),
                            nextIndex());
                changeStateTo(new InstallSnapshotState(snapshotContext.get()));
            }
            return null;
        }

        private boolean canSend() {
            return running &&
                    raftPeer.isReadyForAppendEntries() &&
                    (matchIndex.get() == 0 || nextIndex.get() - matchIndex.get() < raftGroup.raftConfiguration().flowBuffer());
        }

        @Override
        public String toString() {
            return "Append Entries Replicator Peer State";
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorPeer.class);

    private final RaftPeer raftPeer;
    private final FluxSink<Long> matchIndexUpdates;
    private final AtomicReference<SnapshotContext> snapshotContext = new AtomicReference<>(new SnapshotContext() {
    });
    private final AtomicLong nextIndex = new AtomicLong(1);
    private final AtomicLong matchIndex = new AtomicLong(0);
    private final AtomicLong lastMessageSent = new AtomicLong(0);
    private final AtomicLong lastMessageReceived = new AtomicLong();
    private volatile boolean running;
    private final Clock clock;
    private final RaftGroup raftGroup;
    private ReplicatorPeerState currentState;
    private final SnapshotManager snapshotManager;
    private final BiConsumer<Long, String> updateCurrentTerm;

    public ReplicatorPeer(RaftPeer raftPeer,
                          FluxSink<Long> matchIndexUpdates,
                          Clock clock,
                          RaftGroup raftGroup,
                          SnapshotManager snapshotManager,
                          BiConsumer<Long, String> updateCurrentTerm,
                          Supplier<Long> lastLogIndex) {
        this.raftPeer = raftPeer;
        this.matchIndexUpdates = matchIndexUpdates;
        this.clock = clock;
        this.updateCurrentTerm = updateCurrentTerm;
        lastMessageReceived.set(clock.millis());
        this.raftGroup = raftGroup;
        this.snapshotManager = snapshotManager;
        this.nextIndex.set(lastLogIndex.get() + 1);
        changeStateTo(new IdleReplicatorPeerState());
    }

    public String nodeId() {
        return raftPeer.nodeId();
    }

    private void changeStateTo(ReplicatorPeerState newState) {
        if (currentState != null) {
            currentState.stop();
        }
        logger.info("{} in term {}: Changing state from {} to {}.", groupId(), currentTerm(), currentState, newState);
        currentState = newState;
        newState.start();
    }

    public void start() {
        logger.info("{} in term {}: Starting the replicator peer for {}.", groupId(), currentTerm(), nodeId());
        running = true;
        matchIndexUpdates.next(matchIndex.get());
        changeStateTo(new AppendEntryState());
    }

    public void stop() {
        logger.info("{} in term {}: Stopping the replicator peer for {}.", groupId(), currentTerm(), nodeId());
        running = false;
        changeStateTo(new IdleReplicatorPeerState());
    }

    public void sendTimeoutNow() {
        raftPeer.sendTimeoutNow();
    }

    public long lastMessageReceived() {
        return lastMessageReceived.get();
    }

    public long lastMessageSent() {
        return lastMessageSent.get();
    }

    public long nextIndex() {
        return nextIndex.get();
    }

    public long matchIndex() {
        return matchIndex.get();
    }

    public int sendNextMessage() {
        return currentState.sendNextEntries();
    }

    private String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    private long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }

    private String me() {
        return raftGroup.localNode().nodeId();
    }

    private long lastAppliedIndex() {
        return raftGroup.logEntryProcessor().lastAppliedIndex();
    }

    private long lastAppliedTerm() {
        return raftGroup.logEntryProcessor().lastAppliedTerm();
    }

    private void setMatchIndex(long newMatchIndex) {
        long matchIndexValue = matchIndex.updateAndGet(old -> (old < newMatchIndex) ? newMatchIndex : old);
        matchIndexUpdates.next(matchIndexValue);
        nextIndex.updateAndGet(currentNextIndex -> Math.max(currentNextIndex, matchIndexValue + 1));
    }
}
