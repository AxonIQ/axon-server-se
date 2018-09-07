package io.axoniq.axonserver.metric;

import com.codahale.metrics.Counter;

/**
 * Created by Sara Pellegrini on 22/06/2018.
 * sara.pellegrini@gmail.com
 */
public class CounterMetric implements ClusterMetric {

    private final Counter counter;

    public CounterMetric(Counter counter) {
        this.counter = counter;
    }

    @Override
    public long size() {
        return counter.getCount();
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
