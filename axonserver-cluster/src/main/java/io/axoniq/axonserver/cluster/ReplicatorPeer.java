package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author Milan Savic
 */
class ReplicatorPeer {

    private static final long FLOW_BUFFER = 100;
    private static final long MAX_ENTRIES_PER_BATCH = 10;

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorPeer.class);

    private final RaftPeer raftPeer;
    private final Consumer<Long> matchIndexCallback;
    private volatile EntryIterator entryIterator;
    private final AtomicLong nextIndex = new AtomicLong(0);
    private final AtomicLong matchIndex = new AtomicLong(0);
    private volatile long lastMessageSent = 0L;
    private volatile long lastMessageReceived;
    private final Clock clock;
    private final RaftGroup raftGroup;
    private final Runnable transitToFollower;

    public long lastMessageReceived() {
        return lastMessageReceived;
    }

    public long nextIndex() {
        return nextIndex.get();
    }

    public long matchIndex() {
        return matchIndex.get();
    }

    public ReplicatorPeer(RaftPeer raftPeer, Consumer<Long> matchIndexCallback, Clock clock,
                          RaftGroup raftGroup, Runnable transitToFollower) {
        this.raftPeer = raftPeer;
        this.matchIndexCallback = matchIndexCallback;
        this.clock = clock;
        lastMessageReceived = clock.millis();
        this.raftGroup = raftGroup;
        this.transitToFollower = transitToFollower;
    }

    private boolean canSend() {
        return matchIndex.get() == 0 || nextIndex.get() - matchIndex.get() < FLOW_BUFFER;
    }

    public int sendNextEntries() {
        int sent = 0;
        try {
            if (entryIterator == null) {
                nextIndex.compareAndSet(0, raftGroup.localLogEntryStore().lastLogIndex() + 1);
                logger.debug("{}: create entry iterator for {} at {}", groupId(), raftPeer.nodeId(), nextIndex);
                entryIterator = raftGroup.localLogEntryStore().createIterator(nextIndex.get());
            }

            if (!canSend()) {
                logger.trace("{}: Trying to send to {} (nextIndex = {}, matchIndex = {}, lastLog = {})",
                                         groupId(),
                                         raftPeer.nodeId(),
                                         nextIndex,
                                         matchIndex,
                                         raftGroup.localLogEntryStore().lastLogIndex());
            }
            while (canSend()
                    && sent < MAX_ENTRIES_PER_BATCH && entryIterator.hasNext()) {
                Entry entry = entryIterator.next();
                //
                TermIndex previous = entryIterator.previous();
                logger.trace("{}: Send request {} to {}: {}", groupId(), sent, raftPeer.nodeId(), entry.getIndex());
                send(AppendEntriesRequest.newBuilder()
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
            if (sent == 0 && now - lastMessageSent > raftGroup.raftConfiguration().heartbeatTimeout()) {
                sendHeartbeat(raftGroup.localLogEntryStore().lastLog(),
                              raftGroup.logEntryProcessor().commitIndex());
            } else {
                if (sent > 0) {
                    lastMessageSent = clock.millis();
                }
            }
        } catch (RuntimeException ex) {
            logger.warn("{}: Sending nextEntries to {} failed", groupId(), raftPeer.nodeId(), ex);
        }
        return sent;
    }

    public void sendHeartbeat(TermIndex lastTermIndex, long commitIndex) {
        AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
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
        lastMessageSent = clock.millis();
    }

    public long getMatchIndex() {
        return matchIndex.get();
    }

    public void handleResponse(AppendEntriesResponse appendEntriesResponse) {
        lastMessageReceived = clock.millis();
        logger.trace("{}: Received response from {}: {}", groupId(), raftPeer.nodeId(), appendEntriesResponse);
        if (appendEntriesResponse.hasFailure()) {
            if (currentTerm() < appendEntriesResponse.getTerm()) {
                logger.info("{}: Replica has higher term: {}", groupId(), appendEntriesResponse.getTerm());
                updateCurrentTerm(appendEntriesResponse.getTerm());
                transitToFollower.run();
                return;
            }
            logger.info("{}: create entry iterator as replica does not have current for {} at {}, lastSaved = {}",
                                    groupId(),
                                    raftPeer.nodeId(),
                                    nextIndex,
                                    appendEntriesResponse.getFailure().getLastAppliedIndex());
            matchIndex.set(appendEntriesResponse.getFailure().getLastAppliedIndex());
            nextIndex.set(appendEntriesResponse.getFailure().getLastAppliedIndex() + 1);
            entryIterator = raftGroup.localLogEntryStore().createIterator(nextIndex.get());
        } else {
            matchIndex.set(appendEntriesResponse.getSuccess().getLastLogIndex());
            matchIndexCallback.accept(matchIndex.get());
        }
    }

    public void handleResponse(InstallSnapshotResponse installSnapshotResponse) {

    }

    private String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    private long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }

    private void updateCurrentTerm(long term) {
        raftGroup.localElectionStore().updateCurrentTerm(term);
    }

    private String me() {
        return raftGroup.localNode().nodeId();
    }
}
