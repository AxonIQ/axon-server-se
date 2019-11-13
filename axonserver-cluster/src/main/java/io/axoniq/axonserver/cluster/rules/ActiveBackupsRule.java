package io.axoniq.axonserver.cluster.rules;

import io.axoniq.axonserver.grpc.cluster.Role;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Rule to check if a condition is true for a minimum number of backup nodes. Returns true if there are no backup nodes
 * available, even if minimum > 0.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class ActiveBackupsRule<N, R> implements Predicate<R> {

    private final Iterable<N> nodesProvider;
    private final Function<N, Role> roleProvider;
    private final BiFunction<N, R, Boolean> test;
    private final Supplier<Integer> minActiveBackupsProvider;

    /**
     * Constructor for the rule.
     *
     * @param nodesProvider            provides the other nodes in the context
     * @param roleProvider             function to retrieve the role for a node
     * @param test                     the condition to test
     * @param minActiveBackupsProvider provides the minumum number of active backups where the condition must be true
     */
    public ActiveBackupsRule(Iterable<N> nodesProvider,
                             Function<N, Role> roleProvider, BiFunction<N, R, Boolean> test,
                             Supplier<Integer> minActiveBackupsProvider) {
        this.nodesProvider = nodesProvider;
        this.roleProvider = roleProvider;
        this.test = test;
        this.minActiveBackupsProvider = minActiveBackupsProvider;
    }

    public boolean test(R value) {
        int score = 0;
        int nodes = 0;
        for (N node : nodesProvider) {
            if (Role.ACTIVE_BACKUP.equals(roleProvider.apply(node))) {
                if (test.apply(node, value)) {
                    score++;
                }
                nodes++;
            }
        }
        return nodes == 0 || score >= minActiveBackupsProvider.get();
    }
}
