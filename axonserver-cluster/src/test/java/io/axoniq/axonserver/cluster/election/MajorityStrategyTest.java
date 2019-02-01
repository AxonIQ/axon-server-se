package io.axoniq.axonserver.cluster.election;

import org.junit.*;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class MajorityStrategyTest {

    @Test
    public void testWonWith3Votes() throws ExecutionException, InterruptedException {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", true);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", true);
        assertTrue(majorityStrategy.isWon().get());
    }

    @Test
    public void testLostWith3Votes() throws ExecutionException, InterruptedException {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", false);
        majorityStrategy.registerVoteReceived("voter3", false);
        assertFalse(majorityStrategy.isWon().get());
    }

    @Test
    public void testWonWith2Votes() throws ExecutionException, InterruptedException {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", true);
        assertTrue(majorityStrategy.isWon().get());
    }

    @Test
    public void testLostWith2Votes() throws ExecutionException, InterruptedException {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", false);
        assertFalse(majorityStrategy.isWon().get());
    }

    @Test
    public void testUncompletedWith1Votes() {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", false);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testUncompletedWith2Votes() {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testDiscardDuplicateVotes() {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        assertFalse(majorityStrategy.isWon().isDone());
    }

    @Test
    public void testWonDiscardingDuplicateVotes() throws ExecutionException, InterruptedException {
        MajorityStrategy majorityStrategy = new MajorityStrategy(() -> 3);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter1", false);
        majorityStrategy.registerVoteReceived("voter2", true);
        majorityStrategy.registerVoteReceived("voter3", true);
        assertTrue(majorityStrategy.isWon().get());
    }
}