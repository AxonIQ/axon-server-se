package io.axoniq.axonserver.metric;

/**
 * @author Marc Gathier
 */
public class FakeClusterMetric implements ClusterMetric {

    private final long count;
    private final double min;
    private final double max;
    private final double mean;
    private final double value;

    public FakeClusterMetric(long count) {
        this(count, 0, 0L, 0L, 0);
    }

    public FakeClusterMetric(long count, double mean) {
        this(count, mean, 0L, 0L, mean);
    }

    public FakeClusterMetric(long count, double value, double min, double max, double mean) {
        this.count = count;
        this.value = value;
        this.min = min;
        this.max = max;
        this.mean = mean;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public double min() {
        return min;
    }

    @Override
    public double max() {
        return max;
    }

    @Override
    public double mean() {
        return mean;
    }

    @Override
    public long count() {
        return count;
    }
}
