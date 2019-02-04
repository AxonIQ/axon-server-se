package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
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

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import static java.lang.String.format;

/**
 * @author Milan Savic
 */
class ReplicatorPeer {


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
    }

    private class InstallSnapshotState implements ReplicatorPeerState {

        private static final int SNAPSHOT_CHUNKS_BUFFER_SIZE = 10;

        private Registration registration;
        private Subscription subscription;
        private int offset;
        private volatile int lastReceivedOffset;
        private volatile boolean done = false;
        private volatile long lastAppliedIndex;

        @Override
        public void start() {
            offset = 0;
            registration = raftPeer.registerInstallSnapshotResponseListener(this::handleResponse);
            lastAppliedIndex = lastAppliedIndex();
            long lastIncludedTerm = lastAppliedTerm();
            snapshotManager.streamSnapshotData(nextIndex(), lastAppliedIndex)
                           .buffer(SNAPSHOT_CHUNKS_BUFFER_SIZE)
                           .subscribe(new Subscriber<List<SerializedObject>>() {
                               @Override
                               public void onSubscribe(Subscription s) {
                                   subscription = s;
                               }

                               @Override
                               public void onNext(List<SerializedObject> serializedObjects) {
                                   InstallSnapshotRequest.Builder requestBuilder =
                                           InstallSnapshotRequest.newBuilder()
                                                                 .setRequestId(UUID.randomUUID().toString())
                                                                 .setGroupId(groupId())
                                                                 .setTerm(currentTerm())
                                                                 .setLeaderId(me())
                                                                 .setLastIncludedTerm(lastIncludedTerm)
                                                                 .setLastIncludedIndex(lastAppliedIndex)
                                                                 .setOffset(offset)
                                                                 .setDone(done)
                                                                 .addAllData(serializedObjects);
                                   logger.trace("{}: Sending install snapshot chunk with offset: {}", groupId(), offset);
                                   if (firstChunk()) {
                                       requestBuilder.setLastConfig(raftGroup.raftConfiguration().config());
                                   }
                                   send(requestBuilder.build());
                                   offset++;
                               }

                               @Override
                               public void onError(Throwable t) {
                                   logger.error("Install snapshot failed.", t);
                                   subscription.cancel();
                                   changeStateTo(new IdleReplicatorPeerState());
                               }

                               @Override
                               public void onComplete() {
                                   done = true;
                                   send(InstallSnapshotRequest.newBuilder()
                                                              .setRequestId(UUID.randomUUID().toString())
                                                              .setGroupId(groupId())
                                                              .setTerm(currentTerm())
                                                              .setLeaderId(me())
                                                              .setLastIncludedTerm(lastIncludedTerm)
                                                              .setLastIncludedIndex(lastAppliedIndex)
                                                              .setOffset(offset)
                                                              .setDone(done)
                                                              .build());
                                   logger.info("{}: Sending the last chunk for install snapshot", groupId());
                               }
                           });
        }

        private boolean firstChunk() {
            return offset == 0;
        }

        @Override
        public void stop() {
            registration.cancel();
        }

        @Override
        public int sendNextEntries() {
            int sent = 0;
            if (!canSend()) {
                logger.trace("Can't send Install Snapshot chunk. offset {}, lastReceivedOffset {}.",
                             offset,
                             lastReceivedOffset);
            }
            while (canSend() && sent < raftGroup.raftConfiguration().maxEntriesPerBatch()) {
                subscription.request(1);
                sent++;
            }
            return sent;
        }

        private void send(InstallSnapshotRequest request) {
            logger.trace("{}: Send request to {}: {}", groupId(), raftPeer.nodeId(), request);
            raftPeer.installSnapshot(request);
            lastMessageSent.getAndUpdate(old -> Math.max(old, clock.millis()));
        }

        public void handleResponse(InstallSnapshotResponse response) {
            logger.trace("{}: Install snapshot - received response: {}", groupId(), response);
            if (response.hasSuccess()) {
                lastMessageReceived.getAndUpdate(old -> Math.max(old, clock.millis()));
                lastReceivedOffset = response.getSuccess().getLastReceivedOffset();
                if (done) {
                    logger.trace("{}: Install snapshot confirmation received: {}", groupId(), response);
                    setMatchIndex(lastAppliedIndex);
                    changeStateTo(new AppendEntryState());
                }
            } else {
                if (currentTerm() < response.getTerm()) {
                    logger.info("{}: Install snapshot - Replica has higher term: {}",
                                groupId(),
                                response.getTerm());
                    String cause = format("%s: %s received InstallSnapshotResponse with term = %s from %s",
                                          groupId(), me(), response.getTerm(), response.getResponseHeader().getNodeId());
                    updateCurrentTerm.accept(response.getTerm(), cause);
                }
            }
        }

        private boolean canSend() {
            return subscription != null &&
                    running &&
                    offset - lastReceivedOffset < raftGroup.raftConfiguration().flowBuffer();
        }
    }

    private class AppendEntryState implements ReplicatorPeerState {

        private volatile EntryIterator entryIterator;
        private Registration registration;

        @Override
        public void start() {
            sendHeartbeat();
            registration = raftPeer.registerAppendEntriesResponseListener(this::handleResponse);
        }

        @Override
        public void stop() {
            registration.cancel();
        }

        @Override
        public int sendNextEntries() {
            int sent = 0;
            try {
                if (entryIterator == null) {
                    nextIndex.compareAndSet(0, raftGroup.localLogEntryStore().lastLogIndex() + 1);
                    logger.debug("{}: create entry iterator for {} at {}", groupId(), raftPeer.nodeId(), nextIndex);
                    updateEntryIterator();
                }

                if (!canSend()) {
                    logger.info("{}: Trying to send to {} (nextIndex = {}, matchIndex = {}, lastLog = {})",
                                 groupId(),
                                 raftPeer.nodeId(),
                                 nextIndex,
                                 matchIndex,
                                 raftGroup.localLogEntryStore().lastLogIndex());
                }
                while (canSend()
                        && sent < raftGroup.raftConfiguration().maxEntriesPerBatch() && entryIterator.hasNext()) {
                    Entry entry = entryIterator.next();
                    //
                    TermIndex previous = entryIterator.previous();
                    logger.trace("{}: Send request {} to {}: {}", groupId(), sent, raftPeer.nodeId(), entry.getIndex());
                    send(AppendEntriesRequest.newBuilder()
                                             .setRequestId(UUID.randomUUID().toString())
                                             .setGroupId(groupId())
                                             .setPrevLogIndex(previous == null ? 0 : previous.getIndex())
                                             .setPrevLogTerm(previous == null ? 0 : previous.getTerm())
                                             .setTerm(currentTerm())
                                             .setLeaderId(me())
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
            } catch (RuntimeException ex) {
                logger.warn("{}: Sending nextEntries to {} failed", groupId(), raftPeer.nodeId(), ex);
            }
            return sent;
        }

        public void handleResponse(AppendEntriesResponse response) {
            logger.trace("{}: Received response from {}: {}", groupId(), raftPeer.nodeId(), response);
            if (response.hasFailure()) {
                if (currentTerm() < response.getTerm()) {
                    logger.info("{}: Replica has higher term: {}", groupId(), response.getTerm());
                    String cause = format("%s: %s received AppendEntriesResponse with term = %s from %s",
                                          groupId(), me(), response.getTerm(), response.getResponseHeader().getNodeId());
                    updateCurrentTerm.accept(response.getTerm(), cause);
                    return;
                }
                logger.info("{}: create entry iterator as replica does not have current for {} at {}, lastSaved = {}, currentMatchIndex = {}",
                            groupId(),
                            raftPeer.nodeId(),
                            nextIndex,
                            response.getFailure().getLastAppliedIndex(),
                            matchIndex());
                setMatchIndex(response.getFailure().getLastAppliedIndex());
                nextIndex.set(matchIndex.get() + 1);
                updateEntryIterator();
            } else {
                lastMessageReceived.getAndUpdate(old -> Math.max(old, clock.millis()));
                setMatchIndex(response.getSuccess().getLastLogIndex());
            }
        }

        public void sendHeartbeat() {
            long commitIndex = raftGroup.logEntryProcessor().commitIndex();
            TermIndex lastTermIndex = raftGroup.localLogEntryStore().lastLog();
            AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                                                                 .setRequestId(UUID.randomUUID().toString())
                                                                 .setCommitIndex(commitIndex)
                                                                 .setLeaderId(me())
                                                                 .setGroupId(raftGroup.raftConfiguration().groupId())
                                                                 .setTerm(raftGroup.localElectionStore()
                                                                                   .currentTerm())
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

        private void updateEntryIterator() {
            try {
                entryIterator = raftGroup.localLogEntryStore().createIterator(nextIndex());
            } catch (IllegalArgumentException iae) {
                logger.info("{}: follower {} is far behind the log entry. Follower's last applied index: {}.",
                            groupId(),
                            raftPeer.nodeId(),
                            nextIndex());
                changeStateTo(new InstallSnapshotState());
            }
        }

        private boolean canSend() {
            return running && matchIndex.get() == 0 || nextIndex.get() - matchIndex.get() < raftGroup.raftConfiguration().flowBuffer();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorPeer.class);

    private final RaftPeer raftPeer;
    private final Consumer<Long> matchIndexCallback;
    private final AtomicLong nextIndex = new AtomicLong(0);
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
                          Consumer<Long> matchIndexCallback,
                          Clock clock,
                          RaftGroup raftGroup,
                          SnapshotManager snapshotManager,
                          BiConsumer<Long, String> updateCurrentTerm) {
        this.raftPeer = raftPeer;
        this.matchIndexCallback = matchIndexCallback;
        this.clock = clock;
        this.updateCurrentTerm = updateCurrentTerm;
        lastMessageReceived.set(clock.millis());
        this.raftGroup = raftGroup;
        this.snapshotManager = snapshotManager;
        changeStateTo(new IdleReplicatorPeerState());
    }

    public String nodeId() {
        return raftPeer.nodeId();
    }

    private void changeStateTo(ReplicatorPeerState newState) {
        if (currentState != null) {
            currentState.stop();
        }
        currentState = newState;
        newState.start();
    }

    public void start() {
        running = true;
        matchIndexCallback.accept(matchIndex.get());
        changeStateTo(new AppendEntryState());
    }

    public void stop() {
        running = false;
        changeStateTo(new IdleReplicatorPeerState());
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
        long newValue = matchIndex.updateAndGet(old -> (old < newMatchIndex) ? newMatchIndex : old);
        matchIndexCallback.accept(newValue);
    }
}
