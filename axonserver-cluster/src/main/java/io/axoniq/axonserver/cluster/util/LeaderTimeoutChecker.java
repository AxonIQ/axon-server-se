package io.axoniq.axonserver.cluster.util;

import io.axoniq.axonserver.cluster.ReplicatorPeerStatus;
import io.axoniq.axonserver.cluster.rules.PrimaryMajorityAndActiveBackupsRule;

import java.time.Clock;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Utility class to check timeouts for a node in leader state. Timeout occurs when node has not heard from majority of
 * the
 * other primary nodes or from a majority of all voting nodes within the maxElectionTimeout.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class LeaderTimeoutChecker {

    private final Iterable<ReplicatorPeerStatus> replicatorPeers;
    private final long maxElectionTimeout;
    private final Supplier<Clock> clock;
    private final Predicate<Long> timeoutRule;

    public LeaderTimeoutChecker(Iterable<ReplicatorPeerStatus> replicatorPeers, long maxElectionTimeout,
                                Supplier<Clock> clock, Supplier<Integer> minActiveBackupsProvider) {
        this.replicatorPeers = replicatorPeers;
        this.maxElectionTimeout = maxElectionTimeout;
        this.clock = clock;
        this.timeoutRule = new PrimaryMajorityAndActiveBackupsRule<>(replicatorPeers, ReplicatorPeerStatus::role,
                                                                     (peer, timeout) -> peer.lastMessageReceived()
                                                                             >= timeout,
                                                                     any -> true,
                                                                     minActiveBackupsProvider);
    }

    /**
     * Checks if the majority of peers has send a message within the timeout.
     *
     * @return an object containing the result of the check and information on why the check failed
     */
    public CheckResult heardFromFollowers() {
        long minTimestamp = clock.get().millis() - maxElectionTimeout;
        if (timeoutRule.test(minTimestamp)) {
            long nextCheck = maxElectionTimeout;
            for (ReplicatorPeerStatus replicatorPeer : replicatorPeers) {
                if (RoleUtils.votingNode(replicatorPeer.role())
                        && replicatorPeer.lastMessageReceived() > minTimestamp) {
                    nextCheck = Math.min(nextCheck, replicatorPeer.lastMessageReceived() - minTimestamp);
                }
            }
            return new CheckResult(true, null, nextCheck);
        }

        return new CheckResult(false, "no messages received", maxElectionTimeout);
    }

    public static class CheckResult {

        private final boolean result;
        private final String reason;
        private final long nextCheckInterval;

        private CheckResult(boolean result, String reason, long nextCheckInterval) {
            this.result = result;
            this.reason = reason;
            this.nextCheckInterval = nextCheckInterval;
        }

        public boolean result() {
            return result;
        }

        public String reason() {
            return reason;
        }

        public long nextCheckInterval() {
            return nextCheckInterval;
        }
    }
}
