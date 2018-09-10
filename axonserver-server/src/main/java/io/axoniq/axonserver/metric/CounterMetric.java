package io.axoniq.axonserver.metric;

import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 22/06/2018.
 * sara.pellegrini@gmail.com
 */
public class CounterMetric implements ClusterMetric {

    private final String name;
    private final Supplier<Long> valueProvider;

    public CounterMetric(String name, Supplier<Long> valueProvider) {
        this.name = name;
        this.valueProvider = valueProvider;
    }

    public String getName() {
        return name;
    }

    @Override
    public long size() {
        return valueProvider.get();
    }

    @Override
    public long min() {
        return size();
    }

    @Override
    public long max() {
        return size();
    }

    @Override
    public double mean() {
        return size();
    }
}
