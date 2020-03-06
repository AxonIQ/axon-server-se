package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.current.CachedCurrentConfiguration;
import io.axoniq.axonserver.cluster.election.DefaultElection;
import io.axoniq.axonserver.cluster.election.DefaultPreVote;
import io.axoniq.axonserver.cluster.election.Election;
import io.axoniq.axonserver.cluster.message.factory.DefaultResponseFactory;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;

/**
 * Abstract state defining common behavior for all Raft states.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public abstract class AbstractMembershipState implements MembershipState {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMembershipState.class);

    private final RaftGroup raftGroup;
    private final StateTransitionHandler transitionHandler;
    private final BiConsumer<Long, String> termUpdateHandler;
    private final MembershipStateFactory stateFactory;
    private final Supplier<Scheduler> schedulerFactory;
    private final Function<Boolean, Election> electionFactory;
    private final Supplier<Election> preElectionFactory;
    private final BiFunction<Integer, Integer, Integer> randomValueSupplier;
    private final SnapshotManager snapshotManager;
    private final CurrentConfiguration currentConfiguration;
    private final Function<Consumer<List<Node>>, Registration> registerConfigurationListener;
    private final RaftResponseFactory raftResponseFactory;

    protected AbstractMembershipState(Builder builder) {
        builder.validate();
        this.raftGroup = builder.raftGroup;
        this.transitionHandler = builder.transitionHandler;
        this.termUpdateHandler = builder.termUpdateHandler;
        this.stateFactory = builder.stateFactory;
        this.schedulerFactory = builder.schedulerFactory;
        this.electionFactory = builder.electionFactory;
        this.preElectionFactory = builder.preVoteFactory;
        this.randomValueSupplier = builder.randomValueSupplier;
        this.snapshotManager = builder.snapshotManager;
        this.currentConfiguration = builder.currentConfiguration;
        this.registerConfigurationListener = builder.registerConfigurationListener;
        this.raftResponseFactory = new DefaultResponseFactory(raftGroup);
    }

    public static abstract class Builder<B extends Builder<B>> {

        public Supplier<Election> preVoteFactory;
        private RaftGroup raftGroup;
        private StateTransitionHandler transitionHandler;
        private BiConsumer<Long, String> termUpdateHandler;
        private MembershipStateFactory stateFactory;
        private Supplier<Scheduler> schedulerFactory;
        private Function<Boolean, Election> electionFactory;
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

        public B termUpdateHandler(BiConsumer<Long, String> termUpdateHandler) {
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

        public B electionFactory(Function<Boolean, Election> electionFactory) {
            this.electionFactory = electionFactory;
            return self();
        }

        public B preVoteFactory(Supplier<Election> preElectionFactory) {
            this.preVoteFactory = preElectionFactory;
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

        public B registerConfigurationListenerFn(
                Function<Consumer<List<Node>>, Registration> registerConfigurationListener) {
            this.registerConfigurationListener = registerConfigurationListener;
            return self();
        }

        protected void validate() {
            if (schedulerFactory == null) {
                schedulerFactory = () -> new DefaultScheduler(raftGroup.localNode().groupId() + "-raftState");
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
                if (registerConfigurationListener == null) {
                    this.registerConfigurationListener = currentConfiguration::registerChangeListener;
                }
            }

            if (electionFactory == null) {
                electionFactory = (disruptLeader) -> {
                    Iterable<RaftPeer> otherPeers = new OtherPeers(raftGroup,
                                                                   currentConfiguration,
                                                                   n -> RoleUtils.votingNode(n.getRole()));
                    return new DefaultElection(raftGroup, termUpdateHandler, otherPeers, disruptLeader);
                };
            }
            if (preVoteFactory == null) {
                preVoteFactory = () -> {
                    Iterable<RaftPeer> otherPeers = new OtherPeers(raftGroup,
                                                                   currentConfiguration,
                                                                   n -> RoleUtils.votingNode(n.getRole()));
                    return new DefaultPreVote(raftGroup, termUpdateHandler, otherPeers);
                };
            }

            if (registerConfigurationListener == null) {
                throw new IllegalStateException("The registerConfigurationListener function must be provided");
            }

            if (snapshotManager == null) {
                throw new IllegalStateException("The snapshotManager must be provided");
            }
        }

        @SuppressWarnings("unchecked")
        private final B self() {
            return (B) this;
        }

        abstract MembershipState build();
    }


    @Override
    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            String message = format("%s received pre-vote with term (%s >= %s) from %s",
                                    me(), request.getTerm(), currentTerm(), request.getCandidateId());
            RequestVoteResponse vote = handleAsFollower(follower -> follower.requestPreVote(request), message);
            logger.info(
                    "{} in term {}: Request for pre-vote received from {} for term {}. {} voted {} (handled as follower).",
                    groupId(),
                    currentTerm(),
                    request.getCandidateId(),
                    request.getTerm(),
                    me(),
                    vote != null && vote.getVoteGranted());
            return vote;
        }
        logger.info("{} in term {}: Request for pre-vote received from {} in term {}. {} voted rejected.",
                    groupId(),
                    currentTerm(),
                    request.getCandidateId(),
                    request.getTerm(),
                    me());
        return responseFactory().voteRejected(request.getRequestId());
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            String message = format("%s received RequestVoteRequest with greater term (%s > %s) from %s",
                                    me(), request.getTerm(), currentTerm(), request.getCandidateId());
            RequestVoteResponse vote = handleAsFollower(follower -> follower.requestVote(request), message);
            logger.info(
                    "{} in term {}: Request for vote received from {} for term {}. {} voted {} (handled as follower).",
                    groupId(),
                    currentTerm(),
                    request.getCandidateId(),
                    request.getTerm(),
                    me(),
                    vote != null && vote.getVoteGranted());
            return vote;
        }
        logger.info("{} in term {}: Request for vote received from {} in term {}. {} voted rejected.",
                    groupId(),
                    currentTerm(),
                    request.getCandidateId(),
                    request.getTerm(),
                    me());
        return responseFactory().voteRejected(request.getRequestId());
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (request.getTerm() > currentTerm()) {
            logger.info(
                    "{} in term {}: Received install snapshot with term {} which is greater than mine. Moving to Follower...",
                    groupId(),
                    currentTerm(),
                    request.getTerm());
            String message = format("%s received InstallSnapshotRequest with greater term (%s > %s) from %s",
                                    me(), request.getTerm(), currentTerm(), request.getLeaderId());
            return handleAsFollower(follower -> follower.installSnapshot(request), message);
        }
        String cause = format("%s in term %s: Received term (%s) is smaller or equal than mine. Rejecting the request.",
                              groupId(), currentTerm(), request.getTerm());
        logger.trace(cause);
        return responseFactory().installSnapshotFailure(request.getRequestId(), cause);
    }

    /**
     * Retrieves the current node definition from the active configuration.
     *
     * @return the current node definition
     */
    protected Node currentNode() {
        return currentGroupMembers().stream().filter(n -> n.getNodeId().equals(me())).findFirst().orElse(null);
    }

    protected boolean shouldGoAwayIfNotMember() {
        return false;
    }

    protected String votedFor() {
        return raftGroup.localElectionStore().votedFor();
    }

    protected void markVotedFor(String candidateId) {
        raftGroup.localElectionStore().markVotedFor(candidateId);
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

    protected void changeStateTo(MembershipState newState, String cause) {
        transitionHandler.updateState(this, newState, cause);
    }

    protected void updateCurrentTerm(long term, String cause) {
        this.termUpdateHandler.accept(term, cause);
    }

    protected int maxElectionTimeout() {
        return raftGroup.raftConfiguration().maxElectionTimeout();
    }

    protected int initialElectionTimeout() {
        return raftGroup.raftConfiguration().initialElectionTimeout();
    }

    protected int minElectionTimeout() {
        return raftGroup.raftConfiguration().minElectionTimeout();
    }

    protected String groupId() {
        return raftGroup().raftConfiguration().groupId();
    }


    protected Stream<RaftPeer> otherPeersStream() {
        return stream(new OtherPeers(raftGroup, currentConfiguration).spliterator(), false);
    }

    protected Election newElection(boolean disruptAllowed) {
        return electionFactory.apply(disruptAllowed);
    }

    protected Election newPreVote() {
        return preElectionFactory.get();
    }

    protected long otherNodesCount() {
        return currentConfiguration.groupMembers().stream()
                                   .filter(node -> !node.getNodeId().equals(me()))
                                   .count();
    }

    protected int random(int min, int max) {
        return randomValueSupplier.apply(min, max);
    }

    protected RaftResponseFactory responseFactory() {
        return raftResponseFactory;
    }

    public CurrentConfiguration currentConfiguration() {
        return this.currentConfiguration;
    }

    protected Registration registerConfigurationListener(Consumer<List<Node>> newConfigurationListener) {
        return registerConfigurationListener.apply(newConfigurationListener);
    }

    protected <R> R handleAsFollower(Function<MembershipState, R> handler, String cause) {
        MembershipState followerState = stateFactory().followerState();
        changeStateTo(followerState, cause);
        return handler.apply(followerState);
    }

    protected <R> R handleAsSecondary(Function<MembershipState, R> handler, String cause) {
        MembershipState secondaryState = stateFactory().secondaryState();
        changeStateTo(secondaryState, cause);
        return handler.apply(secondaryState);
    }

    @Override
    public List<Node> currentGroupMembers() {
        return currentConfiguration.groupMembers();
    }
}
