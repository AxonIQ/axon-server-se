package io.axoniq.axonhub.metric;

import com.codahale.metrics.Snapshot;

/**
 * Created by Sara Pellegrini on 18/04/2018.
 * sara.pellegrini@gmail.com
 */
public class SnapshotMetric implements ClusterMetric {

    private final Snapshot snapshot;

    public SnapshotMetric(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public long size() {
        return snapshot.size();
    }

    @Override
    public long min() {
        return snapshot.getMin();
    }

    @Override
    public long max() {
        return snapshot.getMax();
    }

    @Override
    public double mean() {
        return snapshot.getMean();
    }
}
