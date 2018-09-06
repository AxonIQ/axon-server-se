package io.axoniq.axonhub.metric;

/**
 * Created by Sara Pellegrini on 17/04/2018.
 * sara.pellegrini@gmail.com
 */
public interface ClusterMetric {

    long size();

    long min();

    long max();

    double mean();


}
