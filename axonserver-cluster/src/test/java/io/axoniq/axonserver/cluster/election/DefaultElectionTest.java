package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.FakeRaftPeer;
import io.axoniq.axonserver.cluster.FakeScheduler;
import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import org.junit.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class DefaultElectionTest {

    private DefaultElection election;
    private FakeScheduler fakeScheduler;
    private ElectionStore electionStore;
    private FakeRaftPeer node1;
    private FakeRaftPeer node2;

    @Before
    public void setUp() throws Exception {
        electionStore = new InMemoryElectionStore();
        electionStore.updateCurrentTerm(100);
        RequestVoteRequest prototype = RequestVoteRequest.newBuilder()
                                                         .setTerm(101)
                                                         .setCandidateId("Node0")
                                                         .setLastLogTerm(100)
                                                         .setLastLogIndex(1000)
                                                         .setGroupId("MyGroupId")
                                                         .build();
        BiConsumer<Long, String> updateTerm = (newTerm, cause) -> electionStore.updateCurrentTerm(newTerm);
        fakeScheduler = new FakeScheduler();
        node1 = new FakeRaftPeer(fakeScheduler, "Node1");
        node2 = new FakeRaftPeer(fakeScheduler, "Node2");
        List<RaftPeer> otherPeers = asList(node1, node2);
        election = new DefaultElection(prototype, updateTerm, electionStore, otherPeers, false);
    }

    @Test
    public void testElectionWon() {
        node1.setTerm(101);
        node2.setTerm(101);
        node1.setVoteGranted(true);
        node2.setVoteGranted(true);
        AtomicBoolean won = new AtomicBoolean(false);
        election.result().subscribe(result -> won.set(result.won()));
        fakeScheduler.timeElapses(100);
        assertTrue(won.get());
    }

    @Test
    public void testElectionWonWithOneVoteNotGranted() {
        node1.setTerm(101);
        node2.setTerm(101);
        node1.setVoteGranted(true);
        node2.setVoteGranted(false);
        AtomicBoolean won = new AtomicBoolean(false);
        election.result().subscribe(result -> won.set(result.won()));
        fakeScheduler.timeElapses(100);
        assertTrue(won.get());
    }

    @Test
    public void testElectionWonWithOneVotePrevTerm() {
        node1.setTerm(101);
        node2.setTerm(100);
        node1.setVoteGranted(true);
        node2.setVoteGranted(true);
        AtomicBoolean won = new AtomicBoolean(false);
        election.result().subscribe(result -> won.set(result.won()));
        fakeScheduler.timeElapses(100);
        assertTrue(won.get());
    }

    @Test
    public void testElectionLost() {
        node1.setTerm(101);
        node2.setTerm(101);
        node1.setVoteGranted(false);
        node2.setVoteGranted(false);
        AtomicBoolean won = new AtomicBoolean(true);
        election.result().subscribe(result -> won.set(result.won()));
        fakeScheduler.timeElapses(100);
        assertFalse(won.get());
    }

    @Test
    public void testElectionTimeout() throws InterruptedException {
        AtomicBoolean won = new AtomicBoolean(true);
        election.result().timeout(Duration.ofMillis(100)).subscribe(result -> won.set(result.won()),
                                                                    error -> won.set(false));
        Thread.sleep(105);
        assertFalse(won.get());
    }

    @Test
    public void testElectionAbortedForGreaterTerm() {
        node1.setTerm(101);
        node2.setTerm(105);
        node1.setVoteGranted(false);
        node2.setVoteGranted(false);
        AtomicBoolean won = new AtomicBoolean(true);
        election.result().subscribe(result -> won.set(result.won()));
        fakeScheduler.timeElapses(100);
        assertFalse(won.get());
    }

}