package io.axoniq.axonserver.cluster;

import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class MinMajority implements Supplier<Integer> {

    private final Supplier<Integer> clusterSize;

    public MinMajority(Supplier<Integer> clusterSize) {
        this.clusterSize = clusterSize;
    }

    @Override
    public Integer get() {
        int size = clusterSize.get();
        return (size / 2) + (size % 2 == 0 ? 0 : 1);
    }
}
