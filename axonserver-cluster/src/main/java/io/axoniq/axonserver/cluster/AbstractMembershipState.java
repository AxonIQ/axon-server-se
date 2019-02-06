package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.current.CachedCurrentConfiguration;
import io.axoniq.axonserver.cluster.election.DefaultElection;
import io.axoniq.axonserver.cluster.election.Election;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.ResponseHeader;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public abstract class AbstractMembershipState implements MembershipState {

    private final RaftGroup raftGroup;
    private final StateTransitionHandler transitionHandler;
    private final BiConsumer<Long,String> termUpdateHandler;
    private final MembershipStateFactory stateFactory;
    private final Supplier<Scheduler> schedulerFactory;
    private final Supplier<Election> electionFactory;
    private final BiFunction<Integer, Integer, Integer> randomValueSupplier;
    private final SnapshotManager snapshotManager;
    private final CurrentConfiguration currentConfiguration;
    private final Function<Consumer<List<Node>>, Registration> registerConfigurationListener;

    protected AbstractMembershipState(Builder builder) {
        builder.validate();
        this.raftGroup = builder.raftGroup;
        this.transitionHandler = builder.transitionHandler;
        this.termUpdateHandler = builder.termUpdateHandler;
        this.stateFactory = builder.stateFactory;
        this.schedulerFactory = builder.schedulerFactory;
        this.electionFactory = builder.electionFactory;
        this.randomValueSupplier = builder.randomValueSupplier;
        this.snapshotManager = builder.snapshotManager;
        this.currentConfiguration = builder.currentConfiguration;
        this.registerConfigurationListener = builder.registerConfigurationListener;
    }

    public static abstract class Builder<B extends Builder<B>> {

        private RaftGroup raftGroup;
        private StateTransitionHandler transitionHandler;
        private BiConsumer<Long, String> termUpdateHandler;
        private MembershipStateFactory stateFactory;
        private Supplier<Scheduler> schedulerFactory;
        private Supplier<Election> electionFactory;
        private BiFunction<Integer, Integer, Integer> randomValueSupplier =
                (min, max) -> ThreadLocalRandom.current().nextInt(min, max);
        private SnapshotManager snapshotManager;
        private CurrentConfiguration currentConfiguration;
        private Function<Consumer<List<Node>>, Registration> registerConfigurationListener;

        public B raftGroup(RaftGroup raftGroup) {
            this.raftGroup = raftGroup;
            return self();
        }

        public B transitionHandler(StateTransitionHandler transitionHandler) {
            this.transitionHandler = transitionHandler;
            return self();
        }

        public B termUpdateHandler(BiConsumer<Long,String> termUpdateHandler) {
            this.termUpdateHandler = termUpdateHandler;
            return self();
        }

        public B stateFactory(MembershipStateFactory stateFactory) {
            this.stateFactory = stateFactory;
            return self();
        }

        public B schedulerFactory(Supplier<Scheduler> schedulerFactory) {
            this.schedulerFactory = schedulerFactory;
            return self();
        }

        public B electionFactory(Supplier<Election> electionFactory) {
            this.electionFactory = electionFactory;
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
            if (schedulerFactory == null) {
                schedulerFactory = DefaultScheduler::new;
            }
            if (raftGroup == null) {
                throw new IllegalStateException("The RAFT group must be provided");
            }
            if (transitionHandler == null) {
                throw new IllegalStateException("The transitionHandler must be provided");
            }
            if (termUpdateHandler == null) {
                throw new IllegalStateException("The termUpdateHandler must be provided");
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

            if (electionFactory == null) {
                electionFactory = () -> {
                    Iterable<RaftPeer> otherPeers = new OtherPeers(raftGroup, currentConfiguration);
                    return new DefaultElection(raftGroup, termUpdateHandler, otherPeers);
                };
            }

            if (registerConfigurationListener == null) {
                throw new IllegalStateException("The registerConfigurationListener function must be provided");
            }

            if (snapshotManager == null){
                throw new IllegalStateException("The snapshotManager must be provided");
            }
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

    public Supplier<Scheduler> schedulerFactory() {
        return schedulerFactory;
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

    protected void changeStateTo(MembershipState newState, String cause) {
        transitionHandler.updateState(this, newState, cause);
    }

    protected void updateCurrentTerm(long term, String cause) {
        this.termUpdateHandler.accept(term, cause);
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

    protected Stream<Node> nodesStream(){
        return currentConfiguration.groupMembers().stream();
    }

    protected Stream<String> otherNodesId(){
        return nodesStream().map(Node::getNodeId).filter(id -> !id.equals(me()));
    }

    protected Stream<RaftPeer> otherPeersStream() {
        return otherNodesId().map(raftGroup::peer);
    }

    protected Election newElection(){
        return electionFactory.get();
    }

    protected long otherNodesCount() {
        return otherNodesId().count();
    }

    protected int random(int min, int max) {
        return randomValueSupplier.apply(min, max);
    }

    protected AppendEntriesResponse appendEntriesFailure(String requestId, String failureCause) {
        AppendEntryFailure failure = AppendEntryFailure.newBuilder()
                                                       .setCause(failureCause)
                                                       .setLastAppliedIndex(lastAppliedIndex())
                                                       .setLastAppliedEventSequence(lastAppliedEventSequence())
                                                       .build();
        return AppendEntriesResponse.newBuilder()
                                    .setResponseHeader(responseHeader(requestId))
                                    .setGroupId(groupId())
                                    .setTerm(currentTerm())
                                    .setFailure(failure)
                                    .build();
    }

    protected InstallSnapshotResponse installSnapshotFailure(String requestId, String cause) {
        return InstallSnapshotResponse.newBuilder()
                                      .setResponseHeader(responseHeader(requestId))
                                      .setGroupId(groupId())
                                      .setTerm(currentTerm())
                                      .setFailure(InstallSnapshotFailure.newBuilder()
                                                                        .setCause(cause)
                                                                        .build())
                                      .build();
    }

    protected RequestVoteResponse requestVoteResponse(String requestId, boolean voteGranted) {
        return requestVoteResponse(requestId, voteGranted, false);
    }

    protected RequestVoteResponse requestVoteResponse(String requestId, boolean voteGranted, boolean goAway) {
        return RequestVoteResponse.newBuilder()
                                  .setResponseHeader(responseHeader(requestId))
                                  .setGroupId(groupId())
                                  .setVoteGranted(voteGranted)
                                  .setTerm(currentTerm())
                                  .setGoAway(goAway)
                                  .build();
    }


    protected ResponseHeader responseHeader(String requestId){
        return ResponseHeader.newBuilder()
                             .setRequestId(requestId)
                             .setResponseId(UUID.randomUUID().toString())
                             .setNodeId(me()).build();
    }

    protected CurrentConfiguration currentConfiguration(){
        return this.currentConfiguration;
    }

    protected Registration registerConfigurationListener(Consumer<List<Node>> newConfigurationListener){
        return registerConfigurationListener.apply(newConfigurationListener);
    }

    @Override
    public List<Node> currentGroupMembers() {
        return currentConfiguration.groupMembers();
    }
}
