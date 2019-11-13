package io.axoniq.axonserver.cluster.rules;

import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.Role;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Rule to check if a condition is true for the majority of primary nodes.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class PrimaryMajorityRule<N, R> implements Predicate<R> {

    private final Iterable<N> nodesProvider;
    private final Function<N, Role> roleProvider;
    private final BiFunction<N, R, Boolean> test;
    private final Predicate<R> selfTest;

    /**
     * Constructor for the rule
     *
     * @param nodesProvider provides all other nodes in the context
     * @param roleProvider  function to get a role for a node
     * @param test          the condition to check
     * @param selfTest      condition to determine if current node passes test
     */
    public PrimaryMajorityRule(Iterable<N> nodesProvider,
                               Function<N, Role> roleProvider, BiFunction<N, R, Boolean> test,
                               Predicate<R> selfTest) {
        this.nodesProvider = nodesProvider;
        this.roleProvider = roleProvider;
        this.test = test;
        this.selfTest = selfTest;
    }


    /**
     * Tests the condition on all nodes to see if it is true for a majority of the primary nodes.
     *
     * @param value value to check in the condition
     * @return true if condition is true for majority of the primary nodes
     */
    public boolean test(R value) {
        int score = selfTest.test(value) ? 1 : 0;
        int nodes = 1;
        for (N node : nodesProvider) {
            if (RoleUtils.primaryNode(roleProvider.apply(node))) {
                if (test.apply(node, value)) {
                    score++;
                }
                nodes++;
            }
        }
        return absoluteMajority(nodes, score);
    }

    private boolean absoluteMajority(int totalNodes, int matchingNodes) {
        return matchingNodes >= Math.ceil(totalNodes / 2f) + (totalNodes % 2 == 0 ? 1 : 0);
    }
}
