package io.axoniq.axonserver.metric;

/**
 * Author: marc
 */
public class FakeClusterMetric implements ClusterMetric {

    private final long size;
    private final long min;
    private final long max;
    private final double mean;

    public FakeClusterMetric(long size) {
        this(size, 0L, 0L, 0);
    }

    public FakeClusterMetric(long size, double mean) {
        this(size,0L,0L, mean);
    }

    public FakeClusterMetric(long size, long min, long max, double mean) {
        this.size = size;
        this.min = min;
        this.max = max;
        this.mean = mean;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long min() {
        return min;
    }

    @Override
    public long max() {
        return max;
    }

    @Override
    public double mean() {
        return mean;
    }
}