package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.current.CachedCurrentConfiguration;
import io.axoniq.axonserver.cluster.message.factory.DefaultResponseFactory;
import io.axoniq.axonserver.cluster.replication.MajorityMatchStrategy;
import io.axoniq.axonserver.cluster.replication.MatchStrategy;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class DefaultStateFactory implements MembershipStateFactory {

    private final RaftGroup raftGroup;
    private final StateTransitionHandler transitionHandler;
    private final BiConsumer<Long, String> termUpdateHandler;
    private final Supplier<Scheduler> schedulerFactory;
    private final SnapshotManager snapshotManager;
    private final CurrentConfiguration currentConfiguration;
    private final Function<Consumer<List<Node>>, Registration> registerConfigurationListener;
    private final MatchStrategy matchStrategy;
    private final Supplier<Long> stateVersionSupplier;


    public DefaultStateFactory(RaftGroup raftGroup,
                               StateTransitionHandler transitionHandler,
                               BiConsumer<Long, String> termUpdateHandler,
                               SnapshotManager snapshotManager, Supplier<Long> stateVersionSupplier) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
        this.termUpdateHandler = termUpdateHandler;
        this.snapshotManager = snapshotManager;
        this.schedulerFactory = () -> new DefaultScheduler(raftGroup.localNode().groupId() + "-raftState");
        CachedCurrentConfiguration configuration = new CachedCurrentConfiguration(raftGroup);
        this.currentConfiguration = configuration;
        this.registerConfigurationListener = configuration::registerChangeListener;
        this.matchStrategy = new MajorityMatchStrategy(() -> raftGroup.localLogEntryStore().lastLogIndex(),
                                                       () -> raftGroup.localNode().replicatorPeers(),
                                                       raftGroup.raftConfiguration()::minActiveBackups);
        this.stateVersionSupplier = stateVersionSupplier;
    }

    private MembershipStateFactory stateFactory() {
        MembershipStateFactory stateFactory = raftGroup.localNode().stateFactory();
        return stateFactory != null ? stateFactory : this;
    }

    @Override
    public IdleState idleState(String nodeId) {
        return new IdleState(nodeId, new DefaultResponseFactory(raftGroup));
    }

    @Override
    public MembershipState fatalState() {
        return new FatalState(new DefaultResponseFactory(raftGroup), raftGroup);
    }

    @Override
    public LeaderState leaderState() {
        return LeaderState.builder()
                          .stateVersionSupplier(stateVersionSupplier)
                          .raftGroup(raftGroup)
                          .schedulerFactory(schedulerFactory)
                          .transitionHandler(transitionHandler)
                          .termUpdateHandler(termUpdateHandler)
                          .snapshotManager(snapshotManager)
                          .currentConfiguration(currentConfiguration)
                          .registerConfigurationListenerFn(registerConfigurationListener)
                          .stateFactory(stateFactory())
                          .matchStrategy(matchStrategy)
                          .build();
    }

    @Override
    public FollowerState followerState() {
        return FollowerState.builder()
                            .stateVersionSupplier(stateVersionSupplier)
                            .raftGroup(raftGroup)
                            .schedulerFactory(schedulerFactory)
                            .transitionHandler(transitionHandler)
                            .termUpdateHandler(termUpdateHandler)
                            .snapshotManager(snapshotManager)
                            .currentConfiguration(currentConfiguration)
                            .registerConfigurationListenerFn(registerConfigurationListener)
                            .stateFactory(stateFactory())
                            .build();
    }

    @Override
    public ProspectState prospectState() {
        return ProspectState.builder()
                            .stateVersionSupplier(stateVersionSupplier)
                            .raftGroup(raftGroup)
                            .schedulerFactory(schedulerFactory)
                            .transitionHandler(transitionHandler)
                            .termUpdateHandler(termUpdateHandler)
                            .snapshotManager(snapshotManager)
                            .currentConfiguration(currentConfiguration)
                            .registerConfigurationListenerFn(registerConfigurationListener)
                            .stateFactory(stateFactory())
                            .build();
    }

    @Override
    public SecondaryState secondaryState() {
        return SecondaryState.builder()
                             .stateVersionSupplier(stateVersionSupplier)
                             .raftGroup(raftGroup)
                             .schedulerFactory(schedulerFactory)
                             .transitionHandler(transitionHandler)
                             .termUpdateHandler(termUpdateHandler)
                             .snapshotManager(snapshotManager)
                             .currentConfiguration(currentConfiguration)
                             .registerConfigurationListenerFn(registerConfigurationListener)
                             .stateFactory(stateFactory())
                             .build();
    }

    @Override
    public CandidateState candidateState() {
        return CandidateState.builder()
                             .stateVersionSupplier(stateVersionSupplier)
                             .raftGroup(raftGroup)
                             .schedulerFactory(schedulerFactory)
                             .transitionHandler(transitionHandler)
                             .termUpdateHandler(termUpdateHandler)
                             .snapshotManager(snapshotManager)
                             .currentConfiguration(currentConfiguration)
                             .registerConfigurationListenerFn(registerConfigurationListener)
                             .stateFactory(stateFactory())
                             .build();
    }

    @Override
    public MembershipState preVoteState() {
        return PreVoteState.builder()
                           .stateVersionSupplier(stateVersionSupplier)
                           .raftGroup(raftGroup)
                           .schedulerFactory(schedulerFactory)
                           .transitionHandler(transitionHandler)
                           .termUpdateHandler(termUpdateHandler)
                           .snapshotManager(snapshotManager)
                           .currentConfiguration(currentConfiguration)
                           .registerConfigurationListenerFn(registerConfigurationListener)
                           .stateFactory(stateFactory())
                           .build();
    }
}
