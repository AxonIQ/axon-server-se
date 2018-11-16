package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Author: marc
 */
public class LeaderState extends AbstractMembershipState {

    private final Map<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t= new Thread(r);
            t.setName("Replication-" + LeaderState.this.raftGroup());
            return t;
        }
    });
    private volatile Replicators replicators;
    protected static class Builder extends AbstractMembershipState.Builder<Builder> {
        public LeaderState build(){
            return new LeaderState(this);
        }
    }

    public static Builder builder(){
        return new Builder();
    }

    LeaderState(Builder builder) {
        super(builder);
    }

    @Override
    public void stop() {
        replicators.stop();
        replicators = null;

    }

    @Override
    public void start() {
        replicators = new Replicators();
        executor.submit(() -> replicators.start());
    }


    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        throw new NotImplementedException();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        throw new NotImplementedException();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new NotImplementedException();
    }

    @Override
    public boolean isLeader() {
        return true;
    }

    @Override
    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        return createEntry(currentTerm(), entryType, entryData);
    }

    private CompletableFuture<Void> createEntry(long currentTerm, String entryType, byte[] entryData) {
        CompletableFuture<Void> appendEntryDone = new CompletableFuture<>();
        CompletableFuture<Entry> entryFuture = raftGroup().localLogEntryStore().createEntry(currentTerm, entryType, entryData);
        entryFuture.whenComplete((e, failure) -> {
            if( failure != null) {
                appendEntryDone.completeExceptionally(failure);
            } else {
                replicators.notifySenders();
                pendingEntries.put(e.getIndex(), appendEntryDone);
            }
        });
        return appendEntryDone;
    }

    private class Replicators {
        private final Map<String, PeerInfo> peerInfoMap = new ConcurrentHashMap<>();
        private volatile boolean running = true;
        void stop() {
            running = false;
            notifySenders();
            peerInfoMap.values().forEach(peer-> close(peer));
        }

        private void close(PeerInfo peer) {
        }

        void start() {
            System.out.println("starting");
            TermIndex lastTermIndex = raftGroup().localLogEntryStore().lastLog();
            raftGroup().raftConfiguration().groupMembers().forEach(n -> {
                peerInfoMap.put( n.getNodeId(), new PeerInfo(n, lastTermIndex.getIndex()+1));
            });

            long commitIndex = raftGroup().localLogEntryStore().commitIndex();
            peerInfoMap.values().forEach(peer-> sendHeartbeat(peer, lastTermIndex, commitIndex));

            while( running) {
                System.out.println("running");
                int runsWithoutChanges = 0;
                try {
                    while( runsWithoutChanges < 10) {
                        int sent = peerInfoMap.values().stream().mapToInt(peer-> sendNext(peer)).sum();
                        if( sent == 0) {
                            runsWithoutChanges++ ;
                        } else {
                            runsWithoutChanges = 0;
                        }
                        System.out.println("running: " + runsWithoutChanges);
                    }
                    System.out.println(System.currentTimeMillis() + " waiting");
                    synchronized (peerInfoMap) {
                        if( running) peerInfoMap.wait(5000);
                    }
                    System.out.println(System.currentTimeMillis() + " continue");
                } catch (InterruptedException e) {
                    running = false;
                    Thread.currentThread().interrupt();
                }
            }
        }

        private int sendNext(PeerInfo peer) {
            if( peer.appendEntriesConnection == null) {
                peer.appendEntriesConnection = raftGroup().createReplicationConnection(peer.node.getNodeId(), matchIndex -> updateMatchIndex(peer,matchIndex));
            }

            return peer.appendEntriesConnection.sendNextEntries(peer);
        }

        private void sendHeartbeat(PeerInfo peer, TermIndex lastTermIndex, long commitIndex) {
            AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                                                                 .setCommitIndex(commitIndex)
                                                                 .setCurrentTerm(raftGroup().localElectionStore().currentTerm())
                                                                 .setPrevLogIndex(lastTermIndex.getIndex())
                                                                 .setPrevLogTerm(lastTermIndex.getTerm())
                                                                 .build();
//                                                                 .setGroupId()
//                    .setLeaderId(raftGroup().localNode().)

            if( peer.appendEntriesConnection == null) {
                peer.appendEntriesConnection = raftGroup().createReplicationConnection(peer.node.getNodeId(), matchIndex -> updateMatchIndex(peer,matchIndex));
            }

            peer.appendEntriesConnection.send(heartbeat);
        }

        public void updateMatchIndex(PeerInfo peer, long matchIndex) {
            peer.setMatchIndex(matchIndex);
            long nextCommitCandidate = raftGroup().localLogEntryStore().commitIndex() + 1;
            if( matchIndex < nextCommitCandidate) return;
            while( matchedByMajority( nextCommitCandidate)) {
                System.out.println("Mark committed: " + nextCommitCandidate);
                raftGroup().localLogEntryStore().markCommitted(nextCommitCandidate);
                nextCommitCandidate++;
            }

        }

        private boolean matchedByMajority(long nextCommitCandidate) {
            int majority = (int) Math.ceil(peerInfoMap.size() / 2f);
            return peerInfoMap.values().stream().filter(p -> p.getMatchIndex() >= nextCommitCandidate).count() >= majority;
        }

        void notifySenders() {
            synchronized (peerInfoMap) {
                peerInfoMap.notify();
            }
        }


    }

    public static class PeerInfo {

        private final Node node;
        private volatile long nextIndex;
        private volatile long matchIndex;
        private volatile ReplicationConnection appendEntriesConnection;

        public PeerInfo(Node node, long nextIndex) {
            this.node = node;
            this.nextIndex = nextIndex;
        }

        public Node getNode() {
            return node;
        }

        public long getNextIndex() {
            return nextIndex;
        }

        public void setNextIndex(long nextIndex) {
            this.nextIndex = nextIndex;
        }

        public long getMatchIndex() {
            return matchIndex;
        }

        public void setMatchIndex(long matchIndex) {
            this.matchIndex = matchIndex;
        }
    }
}
