package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.FakeRaftPeer;
import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class PrimaryAndVotingMajorityStrategyTest {

    @Test
    public void testWonWith3Votes() throws ExecutionException, InterruptedException {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", true);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", true);
        assertTrue(majorityStrategy.isWon().get().won());
    }

    @Test
    public void testLostWith3Votes() throws ExecutionException, InterruptedException {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", false);
        majorityStrategy.registerVoteReceived("voter3", false);
        assertFalse(majorityStrategy.isWon().get().won());
    }

    @Test
    public void testWonWith2Votes() throws ExecutionException, InterruptedException {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", true);
        assertTrue(majorityStrategy.isWon().get().won());
    }

    @Test
    public void testLostWith2Votes() throws ExecutionException, InterruptedException {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", false);
        assertFalse(majorityStrategy.isWon().get().won());
    }

    @Test
    public void testUncompletedWith1Votes() {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", false);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testUncompletedWith2Votes() {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testDiscardDuplicateVotes() {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testWonDiscardingDuplicateVotes() throws ExecutionException, InterruptedException {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter1", Role.PRIMARY),
                                             new FakeRaftPeer("voter2", Role.PRIMARY));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", true);
        assertTrue(majorityStrategy.isWon().get().won());
    }

    @Test
    public void testLostWith3VotesOnlyPrimary() {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter2", Role.PRIMARY),
                                             new FakeRaftPeer("voter3", Role.PRIMARY),
                                             new FakeRaftPeer("voter4", Role.ACTIVE_BACKUP),
                                             new FakeRaftPeer("voter5", Role.ACTIVE_BACKUP));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", true);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", true);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testLostWith3VotesOnlyBackupNodes() {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter2", Role.PRIMARY),
                                             new FakeRaftPeer("voter3", Role.PRIMARY),
                                             new FakeRaftPeer("voter4", Role.ACTIVE_BACKUP),
                                             new FakeRaftPeer("voter5", Role.ACTIVE_BACKUP));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", true);
        majorityStrategy.registerVoteReceived("voter4", true);
        majorityStrategy.registerVoteReceived("voter5", true);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testWonWith3VotesPrimaryAndVoters() throws ExecutionException, InterruptedException {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter2", Role.PRIMARY),
                                             new FakeRaftPeer("voter3", Role.PRIMARY),
                                             new FakeRaftPeer("voter4", Role.ACTIVE_BACKUP),
                                             new FakeRaftPeer("voter5", Role.ACTIVE_BACKUP));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", true);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter4", true);
        assertTrue(majorityStrategy.isWon().get().won());
    }

    @Test
    public void testWonWith3VotesPrimaryAndNonVoters() throws ExecutionException, InterruptedException {
        List<RaftPeer> peers = Arrays.asList(new FakeRaftPeer("voter2", Role.PRIMARY),
                                             new FakeRaftPeer("voter3", Role.PRIMARY),
                                             new FakeRaftPeer("voter4", Role.PASSIVE_BACKUP),
                                             new FakeRaftPeer("voter5", Role.PASSIVE_BACKUP));
        PrimaryAndVotingMajorityStrategy majorityStrategy = new PrimaryAndVotingMajorityStrategy(peers);
        majorityStrategy.registerVoteReceived("voter1", true);
        majorityStrategy.registerVoteReceived("voter2", true);
        assertTrue(majorityStrategy.isWon().get().won());
    }
}