package io.axoniq.axonserver.metric;

/**
 * Author: marc
 */
public interface AxonServerMetric {

    long getSize();

    long getMin();

    long getMax();

    double getMean();

    String getName();
}
