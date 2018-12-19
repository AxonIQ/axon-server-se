package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.current.CachedCurrentConfiguration;
import java.util.List;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public abstract class AbstractMembershipState implements MembershipState {

    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;
    private final MembershipStateFactory stateFactory;
    private final Scheduler scheduler;
    private final BiFunction<Integer, Integer, Integer> randomValueSupplier;
    private final SnapshotManager snapshotManager;
    private final CurrentConfiguration currentConfiguration;
    private final Function<Consumer<List<Node>>, Registration> registerConfigurationListener;

    protected AbstractMembershipState(Builder builder) {
        builder.validate();
        this.raftGroup = builder.raftGroup;
        this.transitionHandler = builder.transitionHandler;
        this.stateFactory = builder.stateFactory;
        this.scheduler = builder.scheduler;
        this.randomValueSupplier = builder.randomValueSupplier;
        this.snapshotManager = builder.snapshotManager;
        this.currentConfiguration = builder.currentConfiguration;
        this.registerConfigurationListener = builder.registerConfigurationListener;
    }

    protected <R> R handleAsFollower(Function<MembershipState, R> handler) {
        MembershipState followerState = stateFactory().followerState();
        changeStateTo(followerState);
        return handler.apply(followerState);
    }

    protected int clusterSize() {
        return currentConfiguration.size();
    }

    public static abstract class Builder<B extends Builder<B>> {

        private RaftGroup raftGroup;
        private Consumer<MembershipState> transitionHandler;
        private MembershipStateFactory stateFactory;
        private Scheduler scheduler;
        private BiFunction<Integer, Integer, Integer> randomValueSupplier =
                (min, max) -> ThreadLocalRandom.current().nextInt(min, max);
        private SnapshotManager snapshotManager;
        private CurrentConfiguration currentConfiguration;
        private Function<Consumer<List<Node>>, Registration> registerConfigurationListener;

        public B raftGroup(RaftGroup raftGroup) {
            this.raftGroup = raftGroup;
            return self();
        }

        public B transitionHandler(Consumer<MembershipState> transitionHandler) {
            this.transitionHandler = transitionHandler;
            return self();
        }

        public B stateFactory(MembershipStateFactory stateFactory) {
            this.stateFactory = stateFactory;
            return self();
        }

        public B scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return self();
        }

        public B randomValueSupplier(BiFunction<Integer, Integer, Integer> randomValueSupplier) {
            this.randomValueSupplier = randomValueSupplier;
            return self();
        }

        public B snapshotManager(SnapshotManager snapshotManager) {
            this.snapshotManager = snapshotManager;
            return self();
        }

        public B currentConfiguration(CurrentConfiguration currentConfiguration) {
            this.currentConfiguration = currentConfiguration;
            return self();
        }

        public B registerConfigurationListenerFn(Function<Consumer<List<Node>>, Registration> registerConfigurationListener) {
            this.registerConfigurationListener = registerConfigurationListener;
            return self();
        }

        protected void validate() {
            if (scheduler == null) {
                scheduler = new DefaultScheduler();
            }
            if (raftGroup == null) {
                throw new IllegalStateException("The RAFT group must be provided");
            }
            if (transitionHandler == null) {
                throw new IllegalStateException("The transitionHandler must be provided");
            }
            if (stateFactory == null) {
                throw new IllegalStateException("The stateFactory must be provided");
            }
            if (currentConfiguration == null) {
                CachedCurrentConfiguration currentConfiguration = new CachedCurrentConfiguration(raftGroup);
                this.currentConfiguration = currentConfiguration;
                if (registerConfigurationListener == null){
                    this.registerConfigurationListener = currentConfiguration::registerChangeListener;
                }
            }

            if (registerConfigurationListener == null) {
                throw new IllegalStateException("The registerConfigurationListener function must be provided");
            }
            // TODO: 11/22/2018 require snapshotManager
        }

        @SuppressWarnings("unchecked")
        private final B self() {
            return (B) this;
        }

        abstract MembershipState build();
    }

    protected String votedFor() {
        return raftGroup.localElectionStore().votedFor();
    }

    protected void markVotedFor(String candidateId) {
        raftGroup.localElectionStore().markVotedFor(candidateId);
    }

    protected long lastAppliedIndex() {
        return raftGroup.logEntryProcessor().lastAppliedIndex();
    }

    protected TermIndex lastLog() {
        return raftGroup.localLogEntryStore().lastLog();
    }

    protected long lastLogIndex() {
        return raftGroup.localLogEntryStore().lastLogIndex();
    }

    protected long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }

    String me() {
        return raftGroup.localNode().nodeId();
    }

    protected RaftGroup raftGroup() {
        return raftGroup;
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public MembershipStateFactory stateFactory() {
        return stateFactory;
    }

    protected SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    protected long lastAppliedEventSequence() {
        return raftGroup.lastAppliedEventSequence();
    }

    protected void changeStateTo(MembershipState newState) {
        transitionHandler.accept(newState);
    }

    protected void updateCurrentTerm(long term) {
        if (term > currentTerm()) {
            ElectionStore electionStore = raftGroup.localElectionStore();
            electionStore.updateCurrentTerm(term);
            electionStore.markVotedFor(null);
        }
    }

    protected int maxElectionTimeout() {
        return raftGroup.raftConfiguration().maxElectionTimeout();
    }

    protected int minElectionTimeout() {
        return raftGroup.raftConfiguration().minElectionTimeout();
    }

    protected String groupId() {
        return raftGroup().raftConfiguration().groupId();
    }

    protected Stream<RaftPeer> otherNodesStream() {
        return currentConfiguration.groupMembers().stream()
                                   .map(Node::getNodeId)
                                   .filter(id -> !id.equals(me()))
                                   .map(raftGroup::peer);
    }

    protected Collection<RaftPeer> otherNodes() {
        return otherNodesStream().collect(Collectors.toList());
    }

    protected long otherNodesCount() {
        return otherNodesStream().count();
    }

    protected int random(int min, int max) {
        return randomValueSupplier.apply(min, max);
    }

    protected AppendEntriesResponse appendEntriesFailure() {
        AppendEntryFailure failure = AppendEntryFailure.newBuilder()
                                                       .setLastAppliedIndex(lastAppliedIndex())
                                                       .setLastAppliedEventSequence(lastAppliedEventSequence())
                                                       .build();
        return AppendEntriesResponse.newBuilder()
                                    .setGroupId(groupId())
                                    .setTerm(currentTerm())
                                    .setFailure(failure)
                                    .build();
    }

    protected InstallSnapshotResponse installSnapshotFailure() {
        return InstallSnapshotResponse.newBuilder()
                                      .setGroupId(groupId())
                                      .setTerm(currentTerm())
                                      .setFailure(InstallSnapshotFailure.newBuilder().build())
                                      .build();
    }

    protected RequestVoteResponse requestVoteResponse(boolean voteGranted) {
        return RequestVoteResponse.newBuilder()
                                  .setGroupId(groupId())
                                  .setVoteGranted(voteGranted)
                                  .setTerm(currentTerm())
                                  .build();
    }

    protected CurrentConfiguration currentConfiguration(){
        return this.currentConfiguration;
    }

    protected Registration registerConfigurationListener(Consumer<List<Node>> newConfigurationListener){
        return registerConfigurationListener.apply(newConfigurationListener);
    }
}
