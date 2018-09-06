package io.axoniq.axonhub.rest.svg.mapping;

import io.axoniq.axonhub.cluster.jpa.ClusterNode;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface AxonHub {

    boolean isActive();

    ClusterNode node();

    Iterable<String> contexts();

    Iterable<AxonDB> storage();
}
