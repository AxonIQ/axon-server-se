package io.axoniq.axonserver.cluster.rules;

import io.axoniq.axonserver.grpc.cluster.Role;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Combined rule to check {@link PrimaryMajorityRule} and {@link ActiveBackupsRule}.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class PrimaryMajorityAndActiveBackupsRule<N, R> implements Predicate<R> {

    private List<Predicate<R>> rules = new ArrayList<>();

    /**
     * Constructor for the combined rule, creating the underlying rules.
     *
     * @param nodesProvider            provides the other nodes in the context
     * @param roleProvider             function to get the role for a node
     * @param test                     condition to check
     * @param selfTest                 condition to check for the current node
     * @param minActiveBackupsProvider provides the minimum number of active backups that should match
     */
    public PrimaryMajorityAndActiveBackupsRule(Iterable<N> nodesProvider,
                                               Function<N, Role> roleProvider,
                                               BiFunction<N, R, Boolean> test,
                                               Predicate<R> selfTest,
                                               Supplier<Integer> minActiveBackupsProvider) {
        rules.add(new PrimaryMajorityRule<>(nodesProvider, roleProvider, test, selfTest));
        rules.add(new ActiveBackupsRule<>(nodesProvider, roleProvider, test, minActiveBackupsProvider));
    }

    @Override
    public boolean test(R value) {
        return rules.stream().allMatch(r -> r.test(value));
    }
}
