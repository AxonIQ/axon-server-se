package io.axoniq.axonserver.rest.svg.mapping;

import io.axoniq.axonserver.cluster.jpa.ClusterNode;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface AxonServer {

    boolean isActive();

    ClusterNode node();

    Iterable<String> contexts();

    Iterable<AxonDB> storage();
}
