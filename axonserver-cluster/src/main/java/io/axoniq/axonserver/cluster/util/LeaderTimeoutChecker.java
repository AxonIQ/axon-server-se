package io.axoniq.axonserver.cluster.util;

import io.axoniq.axonserver.cluster.ReplicatorPeerStatus;

import java.time.Clock;
import java.util.function.Supplier;

import static java.lang.String.format;

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

    public LeaderTimeoutChecker(Iterable<ReplicatorPeerStatus> replicatorPeers, long maxElectionTimeout,
                                Supplier<Clock> clock) {
        this.replicatorPeers = replicatorPeers;
        this.maxElectionTimeout = maxElectionTimeout;
        this.clock = clock;
    }

    /**
     * Checks if the majority of peers has send a message within the timeout.
     *
     * @return an object containing the result of the check and information on why the check failed
     */
    public CheckResult heardFromFollowers() {
        TimeoutChecker timeoutChecker = new TimeoutChecker();
        if (!timeoutChecker.primaryMajority()) {
            String message = format("no messages received from majority of primary nodes for %sms",
                                    maxElectionTimeout);
            return new CheckResult(false, message, timeoutChecker.nextCheck);
        }

        if (!timeoutChecker.fullMajority()) {
            String message = format(
                    "no messages received from non-primary nodes for %s ms",
                    maxElectionTimeout);
            return new CheckResult(false, message, timeoutChecker.nextCheck);
        }

        return new CheckResult(true, null, timeoutChecker.nextCheck);
    }

    private class TimeoutChecker {

        int primaryNodes;
        int primaryCount;
        int fullNodes;
        int fullCount;
        long nextCheck = maxElectionTimeout;

        public TimeoutChecker() {
            primaryNodes = 1;
            fullNodes = 1;
            primaryCount = 1;
            fullCount = 1;
            long minTimestamp = clock.get().millis() - maxElectionTimeout;

            for (ReplicatorPeerStatus replicatorPeer : replicatorPeers) {
                if (replicatorPeer.primaryNode()) {
                    primaryNodes++;
                    if (replicatorPeer.lastMessageReceived() >= minTimestamp) {
                        primaryCount++;
                        nextCheck = Math.min(nextCheck, replicatorPeer.lastMessageReceived() - minTimestamp);
                    }
                }
                if (replicatorPeer.votingNode()) {
                    fullNodes++;
                    if (replicatorPeer.lastMessageReceived() >= minTimestamp) {
                        fullCount++;
                        nextCheck = Math.min(nextCheck, replicatorPeer.lastMessageReceived() - minTimestamp);
                    }
                }
            }
        }

        public boolean fullMajority() {
            return fullCount >= (int) Math.ceil((fullNodes + 0.1f) / 2f) &&
                    (primaryNodes == fullNodes || fullCount > primaryCount);
        }

        public boolean primaryMajority() {
            return primaryCount >= (int) Math.ceil((primaryNodes + 0.1f) / 2f);
        }
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
